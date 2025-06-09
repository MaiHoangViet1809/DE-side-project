from airflow import DAG # noqa
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.models.param import Param

from cores.models.managers.job_info import get_airflow_metadata
from cores.utils.configs import FrameworkConfigs as cfg
from cores.orchestration.flow_factory import create_flow
import cores.logic_repos.objectives as do
Factory = cfg.Core.FACTORY_CLASS

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]
# ---------------------------------------------------------------------------------------------------------------------
PARAMS = {
    "run_dates": Param([(datetime.now() + relativedelta(days=0)).strftime("%Y-%m-%d"),], type=["object", "array"])
}


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
def Pipelines(msg):
    task_1 = Factory(do.spark_transformation)(
            task_id=f"RAW-TBL_INTERNAL_AIRFLOW_LOGGING",
            pipe_msg=msg,
            task_params=dict(
                source_path=get_airflow_metadata,
                source_format="pandas",
                sink_path="RAW.TBL_INTERNAL_AIRFLOW_LOGGING",

                hook_sink=do.SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
                partition_column="DAG_LOGICAL_DATE",
                partition_value="[[ run_date ]]",
                refresh_type=do.RefreshType.partition_n_days,
                transform_callables=[
                    lambda df: df.withColumns(dict(
                        DAG_LOGICAL_DATE=do.to_timestamp(df.DAG_LOGICAL_DATE, "yyyy-MM-dd"),
                    )),
                ],

                ignore_logging=True,
            ),
        )


my_dag = create_flow(
    schedule="*/30 * * * *",  # every 30 minute
    dag_id=MODULE_NAME,

    start_date=datetime(2022, 1, 1),
    catchup=False,

    steps=Pipelines,
    params=PARAMS,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["INGESTION", "AIRFLOW", "LOGGING"],
    file_name=__file__,
)
