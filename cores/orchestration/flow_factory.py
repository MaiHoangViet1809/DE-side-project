from typing import Callable, Dict, List, Tuple, Literal

from airflow.decorators import dag, task_group
from airflow.utils.trigger_rule import TriggerRule

from cores.orchestration.dag_preconfigure import handle_configure
from cores.orchestration.email_handle import EmailFailure
import cores.orchestration.components as com
from cores.utils.configs import FrameworkConfigs as cfg


def create_flow(steps: Dict[str, Tuple[Callable, Dict]] | Callable = None,
                **configure):
    """
    Main function to generate an Airflow pipeline (DAG) from callable steps.

    Args:
        steps (Dict[str, Tuple[Callable, Dict]] | Callable, optional): The pipeline steps. Can be a dictionary where keys
            represent task names and values are tuples of (function, parameters), or a single callable function.
        **configure: Additional configuration for the DAG.

    Returns:
        Callable: The created Airflow DAG pipeline.

    Examples:
        ```python
        from airflow.decorators import task

        @task
        def step_1(pipe_msg, task_params):
            # Process step 1
            return "Step 1 completed"

        @task
        def step_2(pipe_msg, task_params):
            # Process step 2
            return "Step 2 completed"

        steps = {
            "Task1": (step_1, {"param1": "value1"}),
            "Task2": (step_2, {"param2": "value2"})
        }

        dag = create_flow(steps=steps, dag_id="example_dag", start_date="2023-01-01", schedule_interval="@daily")
        ```
    """
    Factory = cfg.Core.FACTORY_CLASS

    version = "1.1.0"

    # tagging config
    configure = handle_configure(version=version, on_failure_callback=EmailFailure(), **configure)

    @dag(**configure)
    def create_pipeline():
        @task_group
        def Configuration():
            t_msg = com.get_configs()
            return t_msg

        @task_group
        def Pipeline(msg):
            tasks = []
            for idx, data in enumerate(steps.items() if isinstance(steps, dict) else steps):
                prev_msg = msg if idx == 0 else tasks[-1]

                # Pipeline ingestion AUTOMATION
                k, (do, task_params) = data

                tasks += [
                    Factory(do)(
                        task_id=f"Refresh-{k.replace('.', '-')}",
                        pipe_msg=prev_msg,
                        task_params=task_params,
                    )
                ]
            return tasks[-1]

        # initializing all group/task --------------------------------------------------------------
        config = Configuration()

        if isinstance(steps, Callable):
            transform = task_group(steps).expand(msg=config)
        else:
            transform = Pipeline.expand(msg=config)

    return create_pipeline()


if __name__ == '__main__':
    print("test")
