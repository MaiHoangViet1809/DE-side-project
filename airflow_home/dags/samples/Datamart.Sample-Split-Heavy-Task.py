from airflow import DAG # noqa
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

from airflow.models.param import Param

from cores.utils.configs import FrameworkConfigs as cfg
from cores.orchestration.flow_factory import create_flow
import cores.logic_repos.objectives as do
Factory = cfg.Core.FACTORY_CLASS

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]
# ---------------------------------------------------------------------------------------------------------------------
PARAMS = {
    "run_dates": Param([
        (datetime.now() + relativedelta(days=0)).strftime("%Y-%m-%d"),
    ], type=["object", "array"]),
}


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
def Pipelines(msg):
    t_step_0 = Factory(do.spark_transformation)(
        task_id=f"Step-Prepare-FACT_SALES",
        pipe_msg=msg,
        task_params=dict(
            source_path="DM.SAMPLE_STEP_0.sql",
            sink_path="DM.TBL_SAMPLE_STEP0_FACT_SALES",
            # partition_column="REPORT_MONTH",
            # partition_value="[[ run_date ]]",
            refresh_type=do.RefreshType.full,
        ),
    )

    t_step_1 = Factory(do.spark_transformation)(
        task_id=f"Step-Prepare-FACT_SALES",
        pipe_msg=t_step_0,
        task_params=dict(
            source_path="DM.SAMPLE_STEP_1.sql",
            sink_path="DM.TBL_SAMPLE_STEP1_FINAL_OUTPUT",
            partition_column="REPORT_MONTH",
            partition_value="[[ run_date ]]",
            refresh_type=do.RefreshType.partition_mtd,
        ),
    )


my_dag = create_flow(
    schedule=None,
    dag_id=MODULE_NAME,

    start_date=datetime(2022, 1, 1),
    catchup=False,

    steps=Pipelines,
    params=PARAMS,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["Sample"],
    file_name=__file__,
)
