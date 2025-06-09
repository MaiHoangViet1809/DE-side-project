from cores.hooks.mssql import SQLServerHook
from cores.engines.spark_engine import SparkEngine
from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.providers import ProvideBenchmark

from pyspark.sql import DataFrame
from uuid import uuid4
from datetime import datetime
from functools import partial


@ProvideBenchmark  # noqa
def mirror_data(**kwargs):
    print("[mirror_data] clone data from Old DB to New DB (UAT)")
    transfer = SparkEngine(
        source_path=kwargs.get('source_path'),
        source_format=kwargs.get('source_format'),
        sink_path=kwargs.get('sink_path'),
        sink_format=kwargs.get('sink_format'),
    )
    transfer.transform(hook=SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
                       hook_sink=SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
                       write_mode="overwrite")

    # print("[mirror_data] clone data from UAT to PROD")
    # transfer.source_path = transfer.sink_path
    # transfer.source_format = transfer.sink_format
    # print(f"[mirror_data] {transfer.source_path=}, {transfer.source_format=}")
    # transfer.transform(hook=SQLServerHook(cfg.Hooks.MSSQL.uat, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
    #                    hook_sink=SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_INGEST),
    #                    write_mode="overwrite")


def add_info(df: DataFrame, uuid, filename):
    return df.withColumns({
        "BATCH_JOB_ID": uuid,
        "EDW_UPDATE_DTTM": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "FILENAME": filename
    })


if __name__ == "__main__":
    UUID = str(uuid4())

    PARAMS = [
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].DDS_INFORMATION"         , "sink_path": "RAW.TBL_DDS_INFORMATION"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[SALES_INFOMATION]"      , "sink_path": "RAW.RAW.TBL_SALES_INFO_DIST"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[ROUTEDSRNAME]"          , "sink_path": "RAW.TBL_ROUTE_SR_NAME"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[ADMIN_LIST]"            , "sink_path": "RAW.TBL_ADMIN_LIST"},
        {"source_path": "[03.CALLTYPE].[dbo].[LIST_MERCHANDISER]"               , "sink_path": "RAW.TBL_MERCHANDISER_LIST"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[ROUTEMERNAME]"          , "sink_path": "RAW.TBL_ROUTE_MER_NAME"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[KC_SALES_INFORMATION]"  , "sink_path": "RAW.TBL_SALES_INFORMATION_KC"},
        {"source_path": "[01.MASTER_MAINTENANCE].[dbo].[Dim_MT_Sale_Talent_Net]", "sink_path": "RAW.TBL_MT_SALE_TALENT_NET"},
    ]

    default_settings = {
        "source_format": "mssql",
        "sink_format": "mssql",
    }

    for table in PARAMS:
        print("start migrating table:", table.get("sink_path"), table)
        table |= default_settings
        mirror_data(**table, apply_transform_func=partial(add_info, uuid=UUID, filename=f"""INIT SYNC FROM {table.get("source_path")}"""))
