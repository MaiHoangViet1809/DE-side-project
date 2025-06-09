from cores.hooks.mssql import SQLServerHook
from cores.engines.spark_engine import SparkEngine
from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.providers import ProvideBenchmark
from cores.models.refresh_type import IngestType, RefreshType
from cores.utils.debug import print_table

from pandas import DataFrame
from uuid import uuid4
from datetime import datetime
from functools import partial
import argparse
from os.path import join


@ProvideBenchmark
def backup_data_to_parquet(source_table: str,
                           sink_path: str,
                           source_partition_column: str = None,
                           **kwargs):
    """
    Backs up data from an MSSQL table to Parquet format, optionally partitioning by a specified column.

    Args:
        source_table (str): The source table name or query to export data from.
        sink_path (str): The destination directory to store Parquet files.
        source_partition_column (str, optional): The column name to use for partitioning the data. Defaults to None.
        **kwargs: Additional keyword arguments for customization.

    Notes:
        - If `source_partition_column` is provided, the data will be partitioned by the specified column, with each
          partition saved as a separate Parquet file.
        - Uses a `SparkEngine` instance to read from the source MSSQL database and write to Parquet.
        - For partitioned exports, processes each partition sequentially and appends the data.
    """
    hook_source = SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST)

    if source_partition_column:
        # add partition info:
        kwargs["partition_column"] = source_partition_column

        print("[backup_data_to_parquet] running df_all_months")
        df_all_months = hook_source.get_pandas_df(f"""
            SELECT SUBSTRING(CONVERT(VARCHAR(10),{source_partition_column}, 127),1, 7) AS RUN_MONTH
              FROM ({source_table}) T
          GROUP BY SUBSTRING(CONVERT(VARCHAR(10),{source_partition_column}, 127),1, 7)
          ORDER BY 1 DESC
        """)

        print_table(df_all_months, 10)

        list_months = df_all_months["RUN_MONTH"].tolist()
        total_months = len(list_months)

        for idx, month in enumerate(list_months):
            print(f"[backup_data_to_parquet] processing {month=} -------------------------- {idx}/{total_months} = {idx / total_months:.2%} -------------------------- ")
            kwargs["partition_value"] = month + "-01"
            source_path = f"""
                SELECT * FROM ({source_table}) T
                 WHERE {kwargs["partition_column"]} BETWEEN CAST(EOMONTH('{kwargs["partition_value"]}', -1) AS DATETIME) + 1
                                                       AND  CAST(EOMONTH('{kwargs["partition_value"]}', 0)  AS DATETIME) + 1 - 1.0/60/60/24 
            """
            transfer = SparkEngine(
                source_path=source_path,
                source_format="mssql",
                sink_path=join(sink_path, kwargs["partition_value"]),
                sink_format="parquet",
            )
            transfer.transform(hook=hook_source, write_mode="overwrite", refresh_type=RefreshType.partition_mtd, **kwargs)
    else:
        transfer = SparkEngine(
            source_path=source_table,
            source_format="mssql",
            sink_path=sink_path,
            sink_format="parquet",
        )
        transfer.transform(hook=hook_source, write_mode="overwrite", **kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cli to backup mssql to parquet")

    parser.add_argument("-source_table", type=str, help="name of sourcing table to be export")
    parser.add_argument("-sink_path", type=str, help="location to export table as parquet file (inside a directory)")
    parser.add_argument("-partition_column", type=str, default=None, help="partition column name to partition data (default None)")

    args = parser.parse_args()
    backup_data_to_parquet(source_table=args.source_table, sink_path=args.sink_path, source_partition_column=args.partition_column)
