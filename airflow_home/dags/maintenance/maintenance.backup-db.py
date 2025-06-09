from airflow import DAG # noqa

import os
from datetime import datetime

from airflow.decorators import task, dag

from cores.orchestration.dag_preconfigure import handle_configure

MODULE_NAME = os.path.splitext(os.path.basename(__file__))[0]


@task(task_id="Backup-DB-EDW")
def process_backup():
    from cores.models.data_models.backup_db import BackupDB
    from cores.hooks.mssql import SQLServerHook
    from cores.utils.configs import FrameworkConfigs as CFG
    from cores.utils.email_misc import list_of_dict_to_html

    print("[process_backup] start backup")
    BackupDB.execute_backup(hook=SQLServerHook(CFG.Hooks.MSSQL.new))

    print("[process_backup] start cleaning up old backup database")
    BackupDB.cleanup_old_backup(n_backups_retention=1)

    print("[process_backup] create email body")
    df = BackupDB.get_records()
    df = df.sort_values(by=["BACKUP_DATETIME"], ascending=[False]).head(1)
    table_as_json = list_of_dict_to_html(df.to_dict('records'))
    return table_as_json


@task(task_id="Send-email")
def send_email(table_data: str):
    from cores.utils.email_smtp import send_email
    from cores.utils.configs import FrameworkConfigs as CFG

    msg_body = "dear all,<br><br>"
    msg_body += f"database {CFG.Core.DB_NAME} has been backup successfully:<br>"
    msg_body += table_data

    send_email(send_to=CFG.Email.ADMIN_DATA_ENGINEER,
               subject="[EDW] BACKUP DB SUCCESS",
               msg_body=msg_body)


@dag(**handle_configure(dag_id=MODULE_NAME,
     schedule="0 21 * * 6",
     start_date=datetime(2024, 5, 1),
     tags=["MAINTENANCE"],
     file_name=__file__,
))
def f_create_dag():
    task_1 = process_backup()
    send_email(table_data=task_1)


my_dag = f_create_dag()
