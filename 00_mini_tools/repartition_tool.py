from cores.utils.configs import FrameworkConfigs as cfg
import cores.logic_repos.objectives as do
from cores.hooks.mssql import SQLServerHook
from uuid import uuid4
import argparse
import pandas as pd


def repartition_table_mssql(target_table: str, partition_column: str, **kwargs):
    """
    Repartitions a Microsoft SQL Server table based on a specified column, creating a new partitioned table.

    Args:
        target_table (str): The name of the target table to be repartitioned.
        partition_column (str): The column used for partitioning the data.
        **kwargs: Additional keyword arguments, including:
            - executor_cores (int): Number of executor cores to use for processing. Default is 8.

    Notes:
        - If a temporary table already exists, compares its partitions with the original table.
        - Creates a temporary table with partitioned data, replacing the old table upon success.
        - The repartitioning process is run in batches, processing data month by month.
        - Uses a Spark transformation pipeline to repartition the data and write it back to the target table.

    Raises:
        ValueError: If the partition column is not specified.
    """
    batch_job_id = str(uuid4())

    old_table = target_table
    new_table = old_table + "_TEMP"

    hook = SQLServerHook(cfg.Hooks.MSSQL.new, database="EDW")

    schema_name, table_name = new_table.split(".")
    if hook.is_table_exists(table_name=table_name, schema_name=schema_name):
        sql = f"""
        WITH OLD_TABLE_INFO AS (
            SELECT CAST(EOMONTH({partition_column}, -1) AS DATETIME) + 1 AS {partition_column}, COUNT(1) AS ROW_NUM 
              FROM {old_table} 
             WHERE {partition_column} IS NOT NULL
          GROUP BY CAST(EOMONTH({partition_column}, -1) AS DATETIME) + 1
        ), NEW_TABLE_INFO AS (
            SELECT CAST(EOMONTH({partition_column}, -1) AS DATETIME) + 1 AS {partition_column}, COUNT(1) AS ROW_NUM 
              FROM {new_table} 
             WHERE {partition_column} IS NOT NULL
          GROUP BY CAST(EOMONTH({partition_column}, -1) AS DATETIME) + 1
        )
        SELECT T.{partition_column}
          FROM OLD_TABLE_INFO AS T 
               LEFT JOIN NEW_TABLE_INFO AS T1
                      ON T.{partition_column} = T1.{partition_column}
         WHERE COALESCE(T.ROW_NUM, 0) <> COALESCE(T1.ROW_NUM, 0)
        """
        print("[repartition_table_mssql] compare sql:", sql)
        data: pd.DataFrame = hook.get_pandas_df(sql=sql)

        print(data.info)
        print(data.dtypes)
        list_date_to_run = pd.to_datetime(data[partition_column]).dt.strftime('%Y-%m-%d').tolist()
    else:
        # get list of month of partition columns
        # order by ascending to run from furthest to current
        list_date_to_run = hook.get_pandas_df(sql=f"""
            SELECT DISTINCT FORMAT({partition_column}, 'yyyy-MM-01') AS {partition_column} 
              FROM {old_table} 
             WHERE {partition_column} IS NOT NULL
          ORDER BY 1
        """)[partition_column].tolist()

    sql_template = f"""
     SELECT *
       FROM {old_table}
      WHERE {partition_column} BETWEEN CAST(EOMONTH('[[ run_date ]]', -1) AS DATETIME) + 1
                                   AND CAST(EOMONTH('[[ run_date ]]', 0)  AS DATETIME) + 1 - 1.0/60/60/24
    """

    for run_date in list_date_to_run:
        print(f"start process for {old_table=} {run_date=}")

        sql = sql_template.replace("[[ run_date ]]", run_date)
        n_rows = do.spark_transformation(
            source_path=sql,
            sink_path=new_table,
            partition_column=partition_column,
            partition_value=run_date,
            refresh_type=do.RefreshType.partition_mtd,

            tableLock=False,
            executor_cores=kwargs.get("executor_cores", 8),

            # need
            environment_name="new",
            batch_job_id=batch_job_id,
            pipe_msg=[dict(environment_name="new")],
            run_date=run_date,

            # this transform will run after data pull from SQL server out to spark environment
            transform_callables=[
                lambda df: df.withColumns({
                    partition_column: do.col(partition_column).cast("timestamp")
                }),
            ],
            # then result will be written back to target table in SQL Server
        )

    print(f"NEW TABLE PARTITIONED {new_table}")
    row_count_old = hook.get_pandas_df(f"SELECT COUNT(1) N_ROWS FROM {old_table}")["N_ROWS"].iloc[0]
    row_count_new = hook.get_pandas_df(f"SELECT COUNT(1) N_ROWS FROM {new_table}")["N_ROWS"].iloc[0]
    print(f"{row_count_old=} {row_count_new=}")
    if row_count_new == row_count_old:
        # replace old table with new table
        hook.run_sql(f"EXEC sp_rename '{old_table}', '{old_table.split('.')[-1]}_BACKUP'")
        hook.run_sql(f"EXEC sp_rename '{new_table}', '{old_table.split('.')[-1]}'")
        hook.run_sql(f"DROP TABLE {old_table}_BACKUP")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cli to repartition table mssql with cci")

    parser.add_argument("--table", "-T", type=str, help="name of table to be repartition")
    parser.add_argument("--partition_column", "-PC", type=str, default=None, help="partition column name to partition data (default None)")
    parser.add_argument("--cores", "-C", type=int, default=8, help="Number of cores")

    args = parser.parse_args()
    repartition_table_mssql(target_table=args.table, partition_column=args.partition_column, executor_cores=args.cores)
