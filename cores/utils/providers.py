import time
from datetime import datetime
import wrapt


# @wrapt.decorator
# def ProvideTime(wrapped, instance, args, kwargs):
#     start_dttm = datetime.now()
#     result = wrapped(*args, **kwargs)
#     end_dttm = datetime.now()
#
#     if isinstance(result, dict):
#         result |= dict(start_dttm=start_dttm, end_dttm=end_dttm)
#     elif result is None:
#         result = dict(start_dttm=start_dttm, end_dttm=end_dttm)
#     return result

def get_hook(conn_id, database, schema):
    """
    Helper function to retrieve a database connection hook.

    Args:
        conn_id (str): Airflow connection name.
        database (str): Name of the database.
        schema (str): Name of the schema.

    Returns:
        SQLServerHook: A hook object for the specified database and schema.
    """
    from cores.hooks.mssql import SQLServerHook
    hook = SQLServerHook(conn_id, database=database, schema=schema)
    return hook


def ProvideHook(conn_id, database: str = "EDW", schema: str = "RAW"):
    """
    Provides a database hook as a decorator for a function.

    Args:
        conn_id (str): Airflow connection name.
        database (str): Default database name (default: "EDW").
        schema (str): Default schema name (default: "RAW").

    Returns:
        Callable: A wrapped function with the hook added to kwargs.
    """

    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        if not kwargs.get("hooks"):
            kwargs["hook"] = get_hook(conn_id, database=database, schema=schema)
        return wrapped(*args, **kwargs)

    return wrapper


@wrapt.decorator
def ProvideBenchmark(wrapped, instance, args, kwargs):
    """
    Logs the benchmarked runtime duration of the decorated function.

    Examples:
        ```python
        @ProvideBenchmark()
        def some_function(*args, **kwargs):
            # do something
        some_function()
        ```
    """
    from cores.models.logs import LogMixin
    start_time = time.time_ns()
    result = wrapped(*args, **kwargs)
    end_time = time.time_ns()
    run_time = (end_time - start_time) / 1e6
    name = getattr(wrapped, '__name__', repr(wrapped))
    LogMixin().msg(f"{name}:: {run_time:,.3f} msec", class_name="ProvideBenchmark")
    return result


@wrapt.decorator
def ProvideHookFramework(wrapped, instance, args, kwargs):
    """
    Decorator that provides a default framework hook.

    Returns:
        Callable: The wrapped function with the hook added to kwargs.
    """

    if not kwargs.get("hook"):
        from cores.utils.configs import FrameworkConfigs as cfg
        kwargs["hook"] = get_hook(cfg.Hooks.MSSQL.new,
                                  database=cfg.Core.DB_NAME,
                                  schema=cfg.Core.SCHEMA_EDW_FRAMEWORK)
    return wrapped(*args, **kwargs)


@wrapt.decorator
def ProvideETLLogging(wrapped, instance, args, kwargs):
    """
    Logs ETL events to the pipeline TableEvent.

    Args:
        kwargs.ignore_logging (bool): If True, skips logging the pipeline event.

    Examples:
        ```python
        @ProvideETLLogging()
        def some_etl_function(*args, **kwargs):
            # perform ETL
        some_etl_function()
        ```
    """
    if not kwargs.get("ignore_logging", False):
        print("[ProvideETLLogging] start")

        # from cores.utils.configs import FrameworkConfigs as cfg
        from cores.models.data_models.event_model import TableEvent, EventType
        from json import dumps

        start_dttm = datetime.now()
        try:
            result = wrapped(*args, **kwargs)
            event_stage = "SUCCESS"
        except Exception as e:
            import traceback
            traceback.print_exc()
            raise e
        end_dttm = datetime.now()

        hook_sink = kwargs.get("hook_sink")  # hook_sink does not exist yet, because this function is a decorator
        if hook_sink:
            conn = hook_sink.get_connection(hook_sink.mssql_conn_id)
            environment_name = conn.host
            if not environment_name: environment_name = "UNKNOWN-1"
        else:
            environment_name = kwargs.get("environment_name", "UNKNOWN-2")

        source_path = kwargs.get("source").data if "source" in kwargs else kwargs.get('source_path')
        sink_path = kwargs.get("sink").data if "sink" in kwargs else kwargs.get("sink_path")
        if not sink_path:
            if transfer := kwargs.get("transfer"):
                sink_path = transfer.sink_path
        batch_job_id = kwargs.get("batch_job_id")
        run_date = kwargs.get("run_date")

        # insert data using ORM SQLAlchemy
        sink = kwargs.get("sink")
        if sink:
            refresh_method = sink.refresh_type.value
        elif "ingest_type" in kwargs:
            refresh_method = str(kwargs.get("ingest_type"))
        elif "refresh_type" in kwargs:
            refresh_method = str(kwargs.get("refresh_type"))
        else:
            refresh_method = None

        # add tagging information
        tags = kwargs.get("tags")

        # source detection
        source_format = kwargs.get("source_format", "")
        if str(source_format).lower() not in ["mssql"] and not ("SELECT " in str(source_path).upper() or "SELECT\n" in str(source_path).upper()):
            source_tables = [source_path]
        else:
            source_tables = [m.uri for m in kwargs.get("inlets", [])]

        record = dict(
            BATCH_JOB_ID=batch_job_id,
            RUN_DATE=run_date,
            ENVIRONMENT_NAME=environment_name,
            TABLE_NAME=sink_path,
            EVENT_NAME=EventType.refresh_data.value,
            EVENT_STAGE=event_stage,
            EVENT_DATA=dumps(dict(
                source_tables=source_tables,
                source_format=source_format,
                refresh_method=refresh_method,
                n_rows=result,
                tags=tags
            )),
            START_DTTM=start_dttm,
            END_DTTM=end_dttm,
        )

        TableEvent.save_record(**record)

    else:
        result = wrapped(*args, **kwargs)
    return result


# def environment(environment_name: str = "production"):
#     @wrapt.decorator
#     def wrapper(wrapped, instance, args, kwargs):
#         kwargs["environment_name"] = environment_name
#         return wrapped(*args, **kwargs)
#
#     return wrapper


@wrapt.decorator
def ProvideErrorLogging(wrapped, instance, args, kwargs):
    """
    Logs errors into a file for debugging purposes.

    Returns:
        Callable: The wrapped function with error logging added.
    """

    result = None
    try:
        result = wrapped(*args, **kwargs)
    except:
        import traceback
        from os import getenv
        AIRFLOW_HOME = getenv("AIRFLOW_HOME")
        with open(AIRFLOW_HOME + "/my_api.log", "a") as f:
            f.write("-" * 50 + datetime.now().strftime("%Y-%m-%d %H:%M:%S") + "-" * 50)
            f.write(traceback.format_exc())
    return result


def ProvideDefault(**defaults):
    """
    Decorator to provide default values for a callable's kwargs.

    Args:
        defaults (dict): Default keyword arguments.

    Returns:
        Callable: The wrapped function with updated kwargs.
    """
    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        for k, v in defaults.items():
            if kwargs.get(k) is None:
                kwargs[k] = v
        return wrapped(*args, **kwargs)
    return wrapper
