from cores.utils.configs import FrameworkConfigs as cfg
from cores.logic_repos.handles.ingestion_handles import HandleDate, HandleDatetimePattern
from cores.hooks.mssql import SQLServerHook

import cores.logic_repos.objectives as do

from uuid import uuid4
from datetime import datetime

Factory = cfg.Core.FACTORY_CLASS

if __name__ == '__main__':
    batch_job_id = str(uuid4())

    run_date = datetime.now().strftime('%Y-%m-%d')

    source_files = [
        # "/mnt/c/Users/Administrator/OneDrive - Kimberly-Clark VietNam/03. FTP ONEDRIVE/RAW DATA KC/01. NW/19.PRIMARY MTD/ORDER DATE/RAW_DATA_PRIMARY_2023-01_2024-05.csv",
        "/mnt/c/Users/Administrator/OneDrive - Kimberly-Clark VietNam/03. FTP ONEDRIVE/RAW DATA KC/01. NW/19.PRIMARY MTD/ORDER DATE/RAW_DATA_PRIMARY_2020_2022.csv",
    ]
    delimiters = [
        # ",",
        "|"
    ]

    target_table = "RAW.TBL_PRIMARY_SALES_ORDER"
    partition_column = "DOCUMENT_DATE"

    temp_table = target_table + "_TEMP"
    hook = SQLServerHook(cfg.Hooks.MSSQL.new, database="EDW")

    for idx, (source_file, delimiter) in enumerate(zip(source_files, delimiters)):
        # ingest into temp table
        print("start ingest file into temp table")
        do.spark_ingestion(
            source_path=source_file,
            source_format="csv",

            sink_path=temp_table,
            sink_format="mssql",
            hook_sink=hook,

            file_name=source_file,

            ingest_type=do.IngestType.masterdata,
            run_date=run_date,

            # need
            environment_name="new",
            batch_job_id=batch_job_id,
            pipe_msg=[dict(environment_name="new")],
            delimiter=delimiter,
            quote="\"",

            # this transform will run after data pull from SQL server out to spark environment
            transform_callables=[
                HandleDatetimePattern(column_names=[partition_column], input_pattern="%Y%m%d"),
            ] if "RAW_DATA_PRIMARY_2020_2022" in source_file else [
                lambda df: df.withColumns({
                    partition_column: do.col(partition_column).cast("timestamp")
                }),
            ]
            ,
            # then result will be written back to target table in SQL Server
        )

        # insert temp to final table
        print("insert temp into partition of table")
        list_date_to_run = hook.get_pandas_df(sql=f"""
            SELECT DISTINCT FORMAT({partition_column}, 'yyyy-MM-01') AS {partition_column} 
              FROM {temp_table} 
             WHERE {partition_column} IS NOT NULL
          ORDER BY 1
        """)[partition_column].tolist()

        sql_template = f"""
         SELECT *
           FROM {temp_table}
          WHERE {partition_column} BETWEEN CAST(EOMONTH('[[ run_date ]]', -1) AS DATETIME) + 1
                                       AND CAST(EOMONTH('[[ run_date ]]', 0)  AS DATETIME) + 1 - 1.0/60/60/24
        """

        for run_date in list_date_to_run:
            print(f"start process for {temp_table=} {target_table=} {run_date=}")

            sql = sql_template.replace("[[ run_date ]]", run_date)
            n_rows = do.spark_transformation(
                source_path=sql,
                sink_path=target_table,
                partition_column=partition_column,
                partition_value=run_date,
                refresh_type=do.RefreshType.partition_mtd,

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

    hook.run_sql(f"DROP TABLE {temp_table}")

