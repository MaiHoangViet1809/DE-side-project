from airflow import DAG # noqa
import os
from datetime import datetime
from dateutil.relativedelta import relativedelta

from cores.logic_repos.objectives.email_task import send_template_email

from cores.utils.configs import FrameworkConfigs as cfg
from cores.orchestration.flow_factory import create_flow
import cores.logic_repos.objectives as do
Factory = cfg.Core.FACTORY_CLASS

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]
# ---------------------------------------------------------------------------------------------------------------------
PARAMS = {
    "run_dates": [
        (datetime.now() + relativedelta(days=0)).strftime("%Y-%m-%d"),
    ],
}


# ---------------------------------------------------------------------------------------------------------------------
# ---------------------------------------------------------------------------------------------------------------------
def Pipelines(msg):
    t_trans_invoice_master = Factory(do.spark_transformation)(
        task_id=f"Report-Test-Data",
        pipe_msg=msg,
        task_params=dict(
            source_path="report.new_test.sql",
            sink_path="DM.TEST_REPORT_DATA",
            partition_column="REPORT_MONTH",
            # partition_value="[[ run_date ]]",
            refresh_type=do.RefreshType.partition_n_days,
        ),
    )

    t_send_email = Factory(send_template_email)(
        task_id="send-email",
        pipe_msg=t_trans_invoice_master,
        task_params=dict(
            send_to=["some-email@gmail.com"],
            subject="[Test Email] - new email",
            cc_to="some-email@gmail.com",
            body_email="test_email.template",
        )
    )


my_dag = create_flow(
    schedule="0 14 * * *",
    dag_id=MODULE_NAME,

    start_date=datetime(2022, 1, 1),
    catchup=False,

    steps=Pipelines,
    params=PARAMS,
    max_active_runs=1,
    max_active_tasks=3,
    tags=["Report", "Sample", "UAT", "DISABLE"]
)
