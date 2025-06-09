from os import getenv as _getenv
from typing import MutableMapping, List, Callable
from importlib import import_module

from cores.orchestration.task_input_process import get_inlets, get_outlets
from cores.utils.env_info import is_in_docker

from dotenv import load_dotenv

load_dotenv()


class ClassProperty:
    """
    Helper class for lazy loading module/object

    Currently implement to lazy load FACTORY_CLASS from cores.models.operators.BaseFramework
    as the result, code's dependency is indirect, only load (lazy load) when DAG is parsed by Airflow Dag Processor
    """
    def __init__(self, f):
        self.f = f

    def __get__(self, obj, owner):
        return self.f(owner)


class FrameworkConfigs:
    """
    Singleton class contain all configuration for the project
    do not instance this class directly
    """

    def __init__(self):
        raise TypeError("This config class is non-instantiable, only using to config the Framework")

    class Core:
        # from cores.models.operators import BaseFramework as _BaseFramework
        from cores.engines import Engine

        DB_NAME = "EDW"
        DB_BACKUP_DIRECTORY = _getenv("DB_BACKUP_DIRECTORY", r"E:\BACKUP-DB")
        # this schema default only exist to ensure pipeline not broken down
        # if any table found under this schema, should move to correct stage of EDW database
        SCHEMA_EDW_DEFAULT = "DBO"

        # main schema list
        SCHEMA_EDW_INGEST = "RAW"
        SCHEMA_EDW_STAGING = "STG"
        SCHEMA_EDW_CONSOLIDATE = "CONS"
        SCHEMA_EDW_DATAMART = "DM"

        SCHEMA_EDW_FRAMEWORK = "CONFIG"

        TABLE_STORE_EVENT = "TBL_FRAMEWORK_EVENT"
        FACTORY_CLASS = ClassProperty(lambda _: getattr(import_module("cores.models.operators"), "BaseFramework"))
        ENGINE_CLASS = Engine.ODBC

        # this callable will run before airflow-task decorator __init__
        TASK_DECOR_CALLBACKS: List[Callable] = [get_inlets,
                                                get_outlets,
                                                ]

        class Jinja:
            DEFAULT_FILTER = ["f_date", "f_date_path", "strptime"]
            DEFAULT_MACRO = ["strptime",
                             "strftime",
                             "f_date",
                             "f_date_path",
                             "f_build_input",
                             "days",
                             "months",
                             "timedelta",
                             "relativedelta",
                             "datetime",
                             int
                             ]

            VAR_START_STRING = "[["
            VAR_END_STRING = "]]"

    class Ingestion:
        ONEDRIVE_LOCATION = _getenv("ONEDRIVE_LOCATION", r"")
        ONEDRIVE_BACKUP_LOCATION = _getenv("ONEDRIVE_BACKUP_LOCATION", r"")

        TEXT_DELIMITER = "|"
        TEXT_SCHEMA_INFER_LENGTH = 50000

        BATCH_SIZE = 50000

        SFTP_MAX_FILE_PER_RUN = 20

    class Email:
        # email of DE assign as admin of this project
        ADMIN_DATA_ENGINEER = ";".join([
            "some-email@gmail.com"
        ])

        KEY_CHART_XCOM = "chart_body"
        KEY_TABLE_XCOM = "email_json_body"
        EXCLUDE_TASKS = ["Finish-Execution"]

        HOST_EMAIL_IP = _getenv("HOST_EMAIL_IP", "")
        HOST_EMAIL_PORT = int(_getenv("HOST_EMAIL_PORT", 25))
        EMAIL_USER = _getenv("EMAIL_USER")
        EMAIL_PASSWORD = _getenv("EMAIL_SECRET")
        EMAIL_CC_TO_DEFAULT = _getenv("EMAIL_CC_TO_DEFAULT")

    class Hooks:
        class Airflow:
            # TODO: check if inside docker
            if is_in_docker():
                BASE_URL = "http://host.docker.internal:8080"
            else:
                BASE_URL = "http://localhost:8080"

            DEFAULT_HEADERS: MutableMapping[str, str] = {"Content-Type": "application/json"}

            # TODO: add handle account to automate airflow
            DEFAULT_AUTH = (_getenv("USERNAME_AIRFLOW_DE_AUTOMATION", ""),
                            _getenv("PASSWORD_AIRFLOW_DE_AUTOMATION", "")
                            )

        class Spark:
            SPARK_MASTER = "local[4]"  # "spark://need-to-set-master-ip:7077"
            SPARK_PACKAGES = []

        class MSSQL:
            production = ""
            new = ""


__all__ = ["FrameworkConfigs"]
