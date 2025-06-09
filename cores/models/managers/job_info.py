from pandas import DataFrame
import pandas as pd
from json import dumps

from datetime import timedelta, datetime as naive_datetime
from pendulum import datetime, instance
from airflow.models import DagBag, TaskInstance, DAG
from airflow.models.dagrun import DagRun

from sqlalchemy.orm import Session

from cores.utils.debug import print_table
from cores.utils.env_info import Env

pd.DataFrame.iteritems = pd.DataFrame.items


def convert_timezone(dt, from_tz="Asia/Bangkok", to_tz="UTC"):
    """
    Converts a datetime object from one timezone to another.

    Args:
        dt: The datetime object to convert.
        from_tz (str): The source timezone. Default is "Asia/Bangkok".
        to_tz (str): The target timezone. Default is "UTC".

    Returns:
        A timezone-aware datetime object in the target timezone.
    """
    return instance(dt, tz=from_tz).in_timezone(to_tz)


def get_airflow_metadata(**kwargs):
    """
    Retrieves metadata about DAG runs and task instances in Airflow.

    This function collects information such as DAG details, task states,
    execution times, and configuration, then structures it into a Pandas DataFrame.

    Args:
        **kwargs: Keyword arguments, including:
            - `run_date` (str): The logical execution date in "YYYY-MM-DD" format.

    Returns:
        DataFrame: A DataFrame containing metadata for DAG runs and tasks.

    Example:
        - Columns include DAG ID, run ID, schedule, tags, file path, run dates,
          configuration, batch job ID, task states, durations, and more.
    """
    framework_home = Env.framework_home()
    dag_bag = DagBag(include_examples=False)
    dag_ids = dag_bag.dag_ids

    run_date = kwargs.get('run_date')  # this run_date will be provided via jinja render custom in run-time
    run_date_tz = naive_datetime.strptime(run_date, "%Y-%m-%d")
    run_date_tz_utc = convert_timezone(run_date_tz)

    runs: list[DagRun] = DagRun.find(dag_ids,
                                     execution_start_date=run_date_tz_utc,
                                     execution_end_date=run_date_tz_utc + timedelta(days=1) + timedelta(seconds=-1),
                                     )
    result = []
    for dr in runs:
        # dag info
        dag: DAG = dag_bag.get_dag(dr.dag_id)
        schedule = str(dag.schedule_interval)
        dag_file_path = dag.fileloc
        #
        tags = dag.tags
        run_type = dr.run_type
        # dag_run info
        dag_run_id = dr.dag_id + "_" + dr.run_id
        dag_config = dr.conf

        list_task_instances: list[TaskInstance] = dr.get_task_instances()
        logical_date = dr.logical_date

        batch_job_id = run_dates = None
        for ti in list_task_instances[:1]:
            params = ti.xcom_pull(task_ids="Configuration.get_configs")
            if params:
                run_dates = params[0][0]["config"]["run_dates"]
                dag_config = params[0][0]["config"]
                batch_job_id = params[0][0]["batch_job_id"]

        if not run_dates: run_dates = dag_config.get("run_dates")
        run_dates = [run_dates] if isinstance(run_dates, str) else run_dates

        for ti in list_task_instances:
            # SNAPSHOT --------------------------------------------------------------------------------------------------------------------------------
            try:
                task = dag.get_task(ti.task_id)
            except:
                task = None

            if task:
                # task_inlets = dumps([m.uri for m in ti.xcom_pull(task_ids=ti.task_id, dag_id=ti.dag_id, key="pipeline_inlets", default=[])])
                task_inlets = dumps([m.uri for m in task.inlets])

                # only get latest outlet
                # task_outlets = [m.uri for m in ti.xcom_pull(task_ids=ti.task_id, dag_id=ti.dag_id, key="pipeline_outlets", default=[])]
                task_outlets = [m.uri for m in task.outlets]
                task_outlets = task_outlets[-1] if len(task_outlets) > 0 else task_outlets
                task_outlets = dumps(task_outlets)
            else:
                task_inlets = ""
                task_outlets = ""

            # -----------------------------------------------------------------------------------------------------------------------------------------
            dag_logical_date = convert_timezone(logical_date, from_tz="UTC", to_tz="Asia/Bangkok").strftime("%Y-%m-%d")
            dag_logical_dttm = convert_timezone(logical_date, from_tz="UTC", to_tz="Asia/Bangkok").strftime("%Y-%m-%d %H:%M:%S %Z")

            result += [dict(
                DAG_LOGICAL_DATE=dag_logical_date,
                DAG_LOGICAL_DTTM=dag_logical_dttm,
                DAG_ID=ti.dag_id,
                DAG_RUN_ID=dag_run_id,
                DAG_SCHEDULE=schedule,
                DAG_TAGS=dumps(tags),
                DAG_FILE_PATH=dag_file_path.removeprefix(framework_home),
                DAG_RUN_DATES=dumps(run_dates),
                DAG_CONFIG=dumps(dag_config),
                DAG_RUN_TYPE=run_type,
                DAG_BATCH_JOB_ID=batch_job_id,
                TASK_ID=ti.task_id,
                TASK_MAP_INDEX=ti.map_index,
                TASK_STATE=ti.state,
                TASK_START_DTTM=convert_timezone(ti.start_date, from_tz="UTC", to_tz="Asia/Bangkok").strftime("%Y-%m-%d %H:%M:%S %Z") if ti.start_date else "",
                TASK_END_DTTM=convert_timezone(ti.end_date, from_tz="UTC", to_tz="Asia/Bangkok").strftime("%Y-%m-%d %H:%M:%S %Z") if ti.end_date else "",
                TASK_DURATION=round(ti.duration, 2) if ti.duration else 0,
                TASK_TRY_NUMBER=ti._try_number if ti._try_number else 0,
                TASK_OPERATOR=ti.operator,
                TASK_INLETS=task_inlets,
                TASK_OUTLETS=task_outlets
            )]

    df = (
        DataFrame(result)
        .assign(
            DAG_START_DTTM=lambda x: x.groupby("DAG_RUN_ID").TASK_START_DTTM.transform("min"),
            DAG_END_DTTM=lambda x: x.groupby("DAG_RUN_ID").TASK_END_DTTM.transform("max"),
        )
        .drop(columns=["DAG_RUN_ID"])
        .sort_values(by=["DAG_ID", "DAG_LOGICAL_DATE", "TASK_MAP_INDEX", "TASK_START_DTTM", "TASK_END_DTTM"])
    )
    # print_table(df.dtypes.reset_index(), 100)
    return df


def get_user_historical(session: Session, dag_id: str, task_id: str, execution_time_utc):
    # from airflow import settings
    from airflow.models import Log
    from sqlalchemy import and_

    # session = settings.Session()
    user_actions = (
        session.query(Log)
        .filter(
            and_(
                Log.dag_id == dag_id,
                Log.task_id == task_id,
                Log.execution_date.between(execution_time_utc, execution_time_utc + timedelta(days=1) + timedelta(seconds=-1))
            )
        )
    ).all()

    # Print out the relevant information
    for action in user_actions:
        print(f"User: {action.owner} - {action.owner_display_name}, Event: {action.event}, Timestamp: {action.dttm} -- {action.execution_date}",
              action.map_index, action.dag_id, action.task_id)

    # Close the session after querying
    session.close()


if __name__ == "__main__":
    # df_test = get_airflow_metadata(run_date="2024-08-07")
    # print_table(df_test, top_n=50, max_col_width=100)
    # print(df_test.DAG_LOGICAL_DATE.min(), df_test.DAG_LOGICAL_DATE.max(), )
    from airflow import settings

    session = settings.Session()

    run_date = "2024-08-29"  # this run_date will be provided via jinja render custom in run-time
    run_date_tz = naive_datetime.strptime(run_date, "%Y-%m-%d")
    run_date_tz_utc = convert_timezone(run_date_tz)

    get_user_historical(session=session,
                        dag_id="DATAMART.CHAMCONGDSR",
                        task_id="Pipelines.Transform-DM-TBL_FACT_CHAMCONGDSR_MONTHLY",
                        execution_time_utc=run_date_tz_utc,)
