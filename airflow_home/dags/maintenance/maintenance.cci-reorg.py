from airflow import DAG  # noqa

import os
from datetime import datetime

from airflow.decorators import task, dag

from cores.orchestration.dag_preconfigure import handle_configure
from cores.utils.debug import print_table

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]


@task(task_id="Build-list-commands")
def get_list():
    from cores.models.managers.maintaince import MSSQLMaintainer
    from cores.hooks.mssql import SQLServerHook
    from cores.utils.configs import FrameworkConfigs as cfg
    hook = SQLServerHook(cfg.Hooks.MSSQL.new)

    maintainer = MSSQLMaintainer(hook=hook)
    list_cmd = maintainer.get_list_cci()
    print("[get_list] table:")
    print_table(list_cmd, 5)

    list_cmd = list_cmd["COMMAND"].values.tolist()
    return list_cmd


@task(task_id="Reorganize-CCI", )
def run_command(cmd: str):
    from cores.models.managers.maintaince import MSSQLMaintainer
    from cores.hooks.mssql import SQLServerHook
    from cores.utils.configs import FrameworkConfigs as cfg
    hook = SQLServerHook(cfg.Hooks.MSSQL.new)
    maintainer = MSSQLMaintainer(hook=hook)
    print(f"[run_command] {cmd=}")
    maintainer.run_command(command=cmd)


@dag(**handle_configure(
    dag_id=MODULE_NAME,
    schedule="0 22 * * *",
    start_date=datetime(2024, 5, 1),
    tags=["MAINTENANCE", "CCI"],
    file_name=__file__,
    max_active_runs=1,
    max_active_tasks=3,
))
def f_create_dag():
    task_1 = get_list()
    task_2 = run_command.expand(cmd=task_1)


my_dag = f_create_dag()
