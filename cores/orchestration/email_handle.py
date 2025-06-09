from abc import abstractmethod

from datetime import datetime, timedelta

from cores.utils.email_misc import DEFAULT_CSS_STYLE, wrap_html_body, list_of_dict_to_html
from cores.utils.email_smtp import send_email
from cores.utils.configs import FrameworkConfigs

from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context


class _IEmailBase:
    """
    Base class for email handling in Airflow.
    Provides a framework for constructing and sending emails based on task and DAG run context.
    """


    _KEY_CHART_XCOM = FrameworkConfigs.Email.KEY_CHART_XCOM
    _KEY_TABLE_XCOM = FrameworkConfigs.Email.KEY_TABLE_XCOM
    _ADMIN_DATA_ENGINEER = FrameworkConfigs.Email.ADMIN_DATA_ENGINEER
    _EXCLUDE_TASKS = FrameworkConfigs.Email.EXCLUDE_TASKS

    def __init__(self, admin_email: str = None):
        if admin_email:
            self.__class__._ADMIN_DATA_ENGINEER = admin_email

    @staticmethod
    def get_default_cc_to():
        """
        Get the default CC email addresses.

        Returns:
            str: Default CC email addresses.
        """
        return FrameworkConfigs.Email.EMAIL_CC_TO_DEFAULT

    @abstractmethod
    def get_context(self, *args, **kwargs) -> Context:
        """
        Abstract method to retrieve the task/DAG context.

        Returns:
            Context: Airflow context object.
        """
        # args[0] if len(args) == 1 else kwargs
        raise NotImplementedError()

    def load_logging_chart(self, ti: TaskInstance):
        """
        Load chart data from XCom.

        Args:
            ti (TaskInstance): The current task instance.

        Returns:
            str: Chart data as an HTML string.
        """
        chart_body = ti.xcom_pull(key=self._KEY_CHART_XCOM)
        return ("<br>" + "<br>".join(chart_body)) if chart_body else None

    def load_logging_table(self, ti: TaskInstance):
        """
        Load table data from XCom and convert it to HTML.

        Args:
            ti (TaskInstance): The current task instance.

        Returns:
            str: Table data as an HTML string.
        """
        email_json_body = ti.xcom_pull(key=self._KEY_TABLE_XCOM)
        if isinstance(email_json_body, str):
            import json
            email_table = json.loads(email_json_body)
        elif not isinstance(email_json_body, (list, dict)) and email_json_body:
            email_table = list(email_json_body)[0]
        else:
            email_table = email_json_body
        return (list_of_dict_to_html(email_table) + "<br>") if email_table else None

    def load_email_body(self, ti: TaskInstance, status_list: list, logical_date: datetime, sorted_all_tasks=None):
        """
        Construct the email body with task statuses and other metadata.

        Args:
            ti (TaskInstance): The current task instance.
            status_list (List[bool]): List of task statuses.
            logical_date (datetime): The logical execution date.
            sorted_all_tasks (List[Dict]): List of all tasks with metadata.

        Returns:
            str: The email body as an HTML string.
        """
        email_body = self.load_logging_table(ti)
        chart_body = self.load_logging_chart(ti)

        msg_body = f"<p class=MsoNormal>Finish status: {sum(status_list)}/{len(status_list)}<o:p></o:p><br>"
        msg_body += f"Run date: {(logical_date + timedelta(days=1)).strftime('%Y-%m-%d %H:%M:%S')}<o:p></o:p><br>"
        msg_body += f"Run id: {ti.run_id}<o:p></o:p></p><br>\n"

        if not email_body:
            email_body = list_of_dict_to_html([{"TASK_ID": m["task_id"],
                                                "MAP_INDEX": m.get("map_index", ""),
                                                "OPERATOR": m["operator"],
                                                "STATE": m["state"] if m["state"] else "",
                                                "START_DATE": (m["start_date"] + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S") if m["start_date"] else "",
                                                "END_DATE": (m["end_date"] + timedelta(hours=7)).strftime("%Y-%m-%d %H:%M:%S") if m["end_date"] else "",
                                                "DURATION": round(m["duration"], 2) if m["duration"] else "",
                                                "TRY_NUMBER": m["_try_number"] or "",
                                                "HOST_NAME": m.get("hostname", ""),
                                                } for m in sorted_all_tasks])

        msg_body += email_body
        msg_body += chart_body or ""

        return wrap_html_body(msg_body, css_style_block=DEFAULT_CSS_STYLE)

    def __call__(self, *args, **kwargs):
        """
        Send an email summarizing the task and DAG run status.

        Args:
            *args: Positional arguments.
            **kwargs: Keyword arguments.

        Returns:
            None
        """
        from airflow.utils.state import State
        from airflow.models.dagrun import DagRun

        context = self.get_context(*args, **kwargs)
        ti: TaskInstance = context['ti']
        logical_date = context["logical_date"] + timedelta(hours=7)
        dag_run: DagRun = ti.get_dagrun()

        all_tasks: list[TaskInstance] = dag_run.get_task_instances()
        sorted_all_tasks = [ti.__dict__ for ti in all_tasks]
        sorted_all_tasks = sorted(sorted_all_tasks, key=lambda e: f'{e["queued_dttm"]}, {e["map_index"]}, {e["task_id"]}')
        sorted_all_tasks = [tis for tis in sorted_all_tasks if tis.get("task_id", tis.get("TASK_ID")) not in self._EXCLUDE_TASKS]

        status_list = [m.state == State.SUCCESS for m in all_tasks]
        check_failed = any([m.state == State.FAILED for m in all_tasks])

        # EMAIL SECTION --------------------------------
        # subject
        job_status = "Failed" if check_failed else "Success"
        subject = f"[{logical_date.strftime('%Y-%m-%d')}][{job_status}] {dag_run.dag_id}"

        # body
        msg_body = self.load_email_body(ti=ti, status_list=status_list, logical_date=logical_date, sorted_all_tasks=sorted_all_tasks)
        msg_body = wrap_html_body(msg_body, css_style_block=DEFAULT_CSS_STYLE)

        # Send email
        params = context.get("params")
        email_to = params.get("email_to", self._ADMIN_DATA_ENGINEER)
        send_email(send_to=email_to,
                   msg_body=msg_body,
                   subject=subject,
                   cc_to=self.get_default_cc_to(),
                   )


class EmailFailure(_IEmailBase):
    """
    Email handler for failure notifications.
    """
    def get_context(self, *args, **kwargs) -> Context:
        if len(args) == 1:
            context = args[0]
        else:
            context = kwargs
        return context


class EmailTask(_IEmailBase):
    """
    Email handler for task-level notifications.
    """
    def get_context(self, *args, **kwargs) -> Context:
        from airflow.operators.python import get_current_context
        return get_current_context()
