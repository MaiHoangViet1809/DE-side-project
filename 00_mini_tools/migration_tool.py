from cores.hooks.mssql import SQLServerHook
from cores.engines.spark_engine import SparkEngine
from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.providers import ProvideBenchmark

from pyspark.sql import DataFrame
from uuid import uuid4
from datetime import datetime

import argparse

@ProvideBenchmark  # noqa
def mirror_data(source_path, sink_path, **kwargs):
    """
    Mirrors data from a source table to a sink table, applying additional transformation logic.

    Args:
        source_path (str): The source table path in the MSSQL database.
        sink_path (str): The sink table path in the MSSQL database.
        **kwargs: Additional keyword arguments for customization of the transformation process.

    Notes:
        - Adds metadata columns such as `BATCH_JOB_ID`, `EDW_UPDATE_DTTM`, and `FILE_NAME`.
        - Uses Spark to extract data from the source and write to the sink.
        - Supports hooks for both source and sink databases.
    """
    UUID = str(uuid4())

    def add_info(df: DataFrame):
        from pyspark.sql.functions import lit
        return df.withColumns({
            "BATCH_JOB_ID": lit(UUID),
            "EDW_UPDATE_DTTM": lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            "FILE_NAME": lit(f"""INIT SYNC FROM {source_path}"""),
        })

    print("[mirror_data] clone data from Old DB to New DB (UAT)")
    transfer = SparkEngine(
        source_path=source_path,
        source_format="mssql",
        sink_path=sink_path,
        sink_format="mssql",
    )
    transfer.transform(hook=SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
                       hook_sink=SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
                       apply_transform_func=[add_info],
                       **kwargs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="cli to migrate table mssql from 179 to 160")

    parser.add_argument("--source_table", "-S", type=str, help="source table")
    parser.add_argument("--sink_table", "-T", type=str, default=None, help="sink table")

    args = parser.parse_args()
    mirror_data(source_path=args.source_table, sink_path=args.sink_table)
