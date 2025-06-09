from datetime import datetime
from dateutil.relativedelta import relativedelta

"""
This module contains a list of functions acting as macros in Jinja2 templates
to render or convert datetime and string values.
"""


def str_to_date(dt):
    """
    Convert a string to a datetime object.

    Args:
        dt (str): The date string in "%Y-%m-%d" format.

    Returns:
        datetime: The converted datetime object.
    """
    return datetime.strptime(dt, "%Y-%m-%d")


def macro_format_date_path(date) -> str:
    """
    Format a date into "YYYY/MM/DD" format.

    Args:
        date (datetime | str): The date to format.

    Returns:
        str: The formatted date.
    """
    if type(date) == str:
        from datetime import datetime
        date = datetime.strptime(date, "%Y-%m-%d")
    return date.strftime('%Y/%m/%d')


def macro_format_date(date) -> str:
    """
    Format a date into "YYYY-MM-DD" format.

    Args:
        date (datetime | str): The date to format.

    Returns:
        str: The formatted date.
    """
    if type(date) == str:
        return date
    return date.strftime('%Y-%m-%d')


def f_build_input(prefix: str, start_date: datetime.date, end_date: datetime.date, mode: str = "days", dt_pattern: str = "%Y/%m/%d"):
    """
    Build a list of paths by appending formatted dates to a prefix.

    Args:
        prefix (str): The base prefix for paths.
        start_date (datetime.date): The start date.
        end_date (datetime.date): The end date.
        mode (str, optional): Increment mode ("days" or "months"). Defaults to "days".
        dt_pattern (str, optional): Date format pattern. Defaults to "%Y/%m/%d".

    Returns:
        str: Comma-separated paths.
    """
    result_date = []
    while start_date <= end_date:
        result_date += [prefix.rstrip("/") + "/" + start_date.strftime(dt_pattern).rstrip("/") + "/"]
        start_date += relativedelta(**{mode: 1})
    listReturn = result_date
    return ",".join(listReturn)


def days(n: int):
    """
    Get a timedelta representing a number of days.

    Args:
        n (int): Number of days.

    Returns:
        relativedelta: Delta of n days.
    """
    return relativedelta(days=n)


def months(n: int):
    """
    Get a timedelta representing a number of months.

    Args:
        n (int): Number of months.

    Returns:
        relativedelta: Delta of n months.
    """
    return relativedelta(months=n)


def f_date(date):
    """
    Format a date into "YYYY-MM-DD" format.

    Args:
        date (datetime | str): The date to format.

    Returns:
        str: The formatted date.
    """
    return macro_format_date(date)


def f_date_path(date):
    """
    Format a date into "YYYY/MM/DD" format.

    Args:
        date (datetime | str): The date to format.

    Returns:
        str: The formatted date path.
    """
    return macro_format_date_path(date)


def strptime(date):
    """
    Convert a string to a datetime object.

    Args:
        date (str): The date string in "%Y-%m-%d" format.

    Returns:
        datetime: The converted datetime object.
    """
    return str_to_date(date)


def macro_get_run_date(prev_task_id: str):
    """
    Get the run_date parameter of a previous task from Airflow XCom.

    Args:
        prev_task_id (str): The name of the previous task.

    Returns:
        str: The run_date parameter.

    Examples:
        ```python
        trigger_dags_secondary_sales_by_invoice = do.TriggerDagRunOperator(
            task_id="Trigger-Dependency-Secondary-Invoice",
            trigger_dag_id="TRANSFORM.SECONDARY-SALES-INVOICE",
            execution_date="{{ macros.datetime.now() }}",
            conf={"run_dates": ["{{ macro_get_run_date('Ingest-SFTP-RAW-TBL_INVOICE_HDR') }}"]},
            reset_dag_run=False,
            wait_for_completion=False,
        )
        ```
    """

    from airflow.operators.python import get_current_context
    ti = get_current_context()["ti"]
    map_index = ti.map_index
    prev_task_id = prev_task_id.removeprefix("Pipelines").removeprefix("PIPELINES")
    print(f"[macro_get_run_date] {prev_task_id=} {map_index=}")
    xcom = ti.xcom_pull(task_ids=f"Pipelines.{prev_task_id}")[map_index]
    first_msg = xcom[0]
    run_date = first_msg["run_date"]
    print(f"[macro_get_run_date] {run_date=}")
    return run_date
