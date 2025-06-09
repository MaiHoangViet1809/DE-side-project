from datetime import timedelta
from typing import Callable
from pandas import DataFrame

from airflow.decorators import task
from airflow.operators.python import get_current_context
from airflow.exceptions import AirflowException, AirflowFailException, AirflowSkipException
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.state import State
from airflow.sensors.python import PythonSensor, PokeReturnValue
from cores.utils.functions import human_readable_size


# def exception_parser(context):
#     ti = context["task_instance"]
#     exception_raised = context.get('exception')
#     print(f"[exception_parser] exception type: {exception_raised.__class__.__name__}", isinstance(exception_raised, AirflowSkipException))
#     if isinstance(exception_raised, AirflowSkipException):
#         ti.set_state(State.SKIPPED)
#     else:
#         print("[exception_parser] Retrying...")


class BaseFramework:
    """
    Base class to convert standard functions into Airflow task decorators with custom implementations
    for input processing and Jinja2 templating.

    Args:
        behavior (Callable, optional): Function to be wrapped into an Airflow task.
    """


    from cores.utils.providers import ProvideBenchmark

    def __init__(self, behavior: Callable = None):
        self.func = behavior
        self.is_sensor = False

    @property
    def behavior_name(self):
        """
        Get the name of the behavior function.

        Returns:
            str: Name of the behavior function.
        """
        return str(getattr(self.func, '__name__', repr(self.func)))

    # -------------------------------------------------------------------------
    # OUTSIDE TASK OPERATION
    # -------------------------------------------------------------------------
    def __call__(self, task_params: dict, pipe_msg, **airflow_kwargs):
        """
        Add default configurations and initialize task decorators.

        Args:
            task_params (dict): Parameters to be passed to the behavior function.
            pipe_msg (list): Message queue from previous tasks.
            **airflow_kwargs: Additional Airflow task configuration parameters.

        Returns:
            Callable: Airflow task instance.
        """
        task_builder = task
        default_config = dict(
            retries=3,
            retry_delay=timedelta(minutes=15),
            max_active_tis_per_dag=6,
            trigger_rule=TriggerRule.ALL_SUCCESS,
            execution_timeout=timedelta(seconds=3600),
        )

        if "poke_interval" in airflow_kwargs:
            default_config = dict(
                poke_interval=airflow_kwargs.get("poke_interval", 60 * 30),
                mode="reschedule",
                timeout=airflow_kwargs.get("timeout", 46800)
            )
            task_builder = task.sensor
            self.is_sensor = True

        airflow_kwargs = {**default_config, **airflow_kwargs}
        task_params, airflow_kwargs = self.apply_modules(task_params, airflow_kwargs)
        task_params = {"task_params": task_params, "pipe_msg": pipe_msg}
        return task_builder(self._process).override(**airflow_kwargs,
                                                    # on_retry_callback=exception_parser,
                                                    )(**task_params)

    @staticmethod
    def apply_modules(task_params: dict, airflow_kwargs: dict):
        """
        Apply pre-defined modules to task parameters.

        Args:
            task_params (dict): Task parameters.
            airflow_kwargs (dict): Airflow task configurations.

        Returns:
            tuple: Updated task_params and airflow_kwargs.
        """
        from cores.orchestration.task_input_process import resolve_file
        from cores.utils.configs import FrameworkConfigs
        from cores.models.jinja import SQLRender

        modules = FrameworkConfigs.Core.TASK_DECOR_CALLBACKS

        task_params = resolve_file(**task_params)

        # pre-render using jinja
        task_params = SQLRender(**task_params).render(task_params)

        if modules:
            for m in modules:
                task_params = m(**task_params)

        # for lineage via Dataset view in airflow
        if "inlets" in task_params:
            airflow_kwargs["inlets"] = task_params["inlets"]

        if "outlets" in task_params:
            airflow_kwargs["outlets"] = task_params["outlets"]

        return task_params, airflow_kwargs

    # -------------------------------------------------------------------------
    # INTERNAL TASK OPERATION
    # -------------------------------------------------------------------------
    @staticmethod
    def render_custom(data: dict, context, **kwargs):
        """
        Render custom templates using both Airflow's Jinja renderer and a custom framework renderer.

        This method applies two levels of rendering to the input data:
        1. Airflow's built-in Jinja renderer
        2. A custom framework Jinja renderer

        Args:
            data (dict): A dictionary containing the data to be rendered. Keys are identifiers,
                         and values are the templates to be rendered.
            context (dict): The Airflow context dictionary, which includes task information
                            and is used by Airflow's Jinja renderer.
            **kwargs: Additional keyword arguments that will be passed to the custom
                      framework renderer as context.

        Returns:
            dict: A new dictionary with the same keys as the input, but with all values
                  rendered through both Airflow's and the custom framework's Jinja renderers.

        Note:
            If 'debug' is set to True in kwargs, the custom framework renderer will print logs.
        """
        from cores.models.jinja import IRender
        airflow_jinja_render = context["task"].render_template
        framework_jinja_render = IRender(context=kwargs)

        data = {k: airflow_jinja_render(v, context=context) for k, v in data.items()}
        data = {k: framework_jinja_render.render(v) for k, v in data.items()}
        if kwargs.get("debug", False): framework_jinja_render.print_logs()
        return data

    def handle_task_params(self, pipe_msg, task_params: dict, context):
        """
        Process and render task parameters.

        Args:
            pipe_msg (list): Message queue from previous tasks.
            task_params (dict): Task parameters.
            context (dict): Airflow context.

        Returns:
            dict: Rendered task parameters.
        """
        from copy import deepcopy
        run_date = pipe_msg[-1]["run_date"]
        params = pipe_msg[0]["config"]  # always get rendered params from first msg
        batch_job_id = pipe_msg[0].get("batch_job_id")

        # get output data
        prev_output = pipe_msg[-1].get("output_data")
        prev_output = deepcopy(prev_output) if isinstance(prev_output, dict) else {}

        status = pipe_msg[-1].get("status")

        # render data
        len_run_date = len(run_date)
        run_date = run_date + "-01" if len(run_date) == 7 else run_date
        data = self.render_custom(data=task_params, context=context,
                                  run_date=run_date,
                                  len_run_date=len_run_date,
                                  # debug=params.get("debug", False),
                                  **params,
                                  )

        # add information
        data |= {
            "run_date": run_date,
            "pipe_msg": pipe_msg,
            "params": params,
            **self._pre_process(prev_output),

            # for transformation - refresh table
            "prev_status": status,
            "batch_job_id": batch_job_id,

            # for clean xcom of auto ingestion pipeline :: cleanup_xcom
            "execution_date": context["ti"].execution_date,

            # add these data for future self reflection pipeline
            "dag_id": context["ti"].dag_id,
            "task_id": context["ti"].task_id,
            "callable_name": self.behavior_name,
            "map_index": getattr(context["ti"], "map_index", None),
        }
        return data

    def try_catch(self, **task_params):
        """
        Execute the behavior function with error handling.

        Args:
            **task_params: Parameters for the behavior function.

        Returns:
            tuple: Return code, output data, and exception info.
        """
        from airflow.exceptions import AirflowTaskTimeout
        exception_info = None
        try:
            output_data = self.func(**task_params)
            rtcode = 0
        except AirflowTaskTimeout as timeout_exception:
            raise timeout_exception
        except Exception as E:
            import traceback
            output_data = traceback.format_exc()
            rtcode = 1
            exception_info = E
        return rtcode, output_data, exception_info

    def _process(self, pipe_msg: list, task_params: dict):
        """
        Internal method for processing tasks.

        Args:
            pipe_msg (list): Message queue from previous tasks.
            task_params (dict): Task parameters.

        Returns:
            list: Updated pipe message queue.
        """
        from cores.utils.configs import FrameworkConfigs

        ctx = get_current_context()
        task_params = self.handle_task_params(pipe_msg=pipe_msg, task_params=task_params, context=ctx)

        engine = task_params.get("engine", FrameworkConfigs.Core.ENGINE_CLASS.value)

        # base process
        return_code, output_data, exception_info = self.try_catch(**task_params)

        final_output_data = output_data
        if isinstance(output_data, PokeReturnValue):
            final_output_data = output_data.xcom_value

        output_msg = {"task_id": task_params["task_id"],
                      "run_date": task_params["run_date"],
                      "map_index": task_params["map_index"],
                      "status": "SUCCESS" if return_code == 0 else "FAILED",
                      "callable": self.behavior_name,
                      "output_data": self._post_process(final_output_data, engine=engine),
                      }

        # sensor operator
        if self.is_sensor:
            if isinstance(output_data, PokeReturnValue):
                if output_data.is_done:
                    pipe_msg += [output_msg]
                    return PokeReturnValue(is_done=True, xcom_value=pipe_msg)
            else:
                if output_data:
                    pipe_msg += [output_msg]
                    return PokeReturnValue(is_done=True, xcom_value=pipe_msg)

            return PokeReturnValue(is_done=False)

        # normal operator
        pipe_msg += [output_msg]
        if return_code == 0:
            return pipe_msg
        else:
            print("Error on " + task_params["task_id"] + f" errors:\n{output_data}")
            raise exception_info

    @staticmethod
    @ProvideBenchmark
    def _post_process(output_data, engine):
        """
        Post-process output data for serialization.

        Args:
            output_data: Output data to process.
            engine (str): Engine type.

        Returns:
            Output data in its final processed form.
        """
        from cores.utils.serde import PDataFrame, LazyFrame, DDataFrame

        # serialize all Dataframe which is not Pandas DataFrame using framework serialize method
        if isinstance(output_data, dict):
            for k, v in output_data.items():
                if engine != "ODBCEngine":
                    if isinstance(v, (PDataFrame, LazyFrame)):
                        output_data[k] = v.lazy().collect().to_pandas()
                    elif isinstance(v, DDataFrame):
                        output_data[k] = v.compute()

                if isinstance(v, DataFrame):
                    size = v.memory_usage(deep=True).sum()
                    print("dataframe size:", human_readable_size(size))

        return output_data

    @staticmethod
    @ProvideBenchmark
    def _pre_process(input_data):
        """
        Pre-process input data for deserialization.

        Args:
            input_data (dict): Input data to process.

        Returns:
            dict: Processed input data.
        """
        from cores.utils.configs import FrameworkConfigs
        engine = input_data.get("engine", FrameworkConfigs.Core.ENGINE_CLASS.value)

        # deserialize data back to Dataframe of engine
        if isinstance(input_data, dict):
            for k, v in input_data.items():
                if isinstance(v, DataFrame):
                    match engine:
                        case "PolarsEngine":
                            from polars import from_pandas
                            input_data[k] = from_pandas(v)
                        case "DaskEngine":
                            from dask.dataframe import from_pandas
                            input_data[k] = from_pandas(v)

        return input_data
