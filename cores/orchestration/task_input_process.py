from cores.utils.env_info import Env

LIST_PARAMS_SOURCE = ["source", "source_path"]


def resolve_file(**kwargs):
    """
    Scan and dynamically resolve `.sql` files to string for given keys in kwargs.

    Args:
        kwargs (dict): Key-value arguments to process.

    Returns:
        dict: Updated kwargs with resolved `.sql` file contents.

    Examples:
        ```python
        kwargs = {"source": "query.sql"}
        resolved_kwargs = resolve_file(**kwargs)
        print(resolved_kwargs["source"])  # Outputs SQL file content as a string.
        ```
    """
    from pathlib import Path

    check_list = [m for m in LIST_PARAMS_SOURCE if m in kwargs]
    if any(check_list):
        airflow_home = Env.airflow_home()
        key_match = check_list[0]
        source = kwargs.get(key_match)
        if isinstance(source, str):
            if source.lower().endswith(".sql"):  # read sql file
                kwargs[key_match] = Path(f"{airflow_home}/dags/sql/{source}").read_text()

    return kwargs


def get_inlets(**kwargs):
    """
    Automatically resolve inlets for a task based on the source path.

    Args:
        kwargs (dict): Key-value arguments to process.

    Returns:
        dict: Updated kwargs with resolved inlets.

    Examples:
        ```python
        kwargs = {"source_path": "SELECT * FROM my_table"}
        updated_kwargs = get_inlets(**kwargs)
        print(updated_kwargs["inlets"])  # Outputs the resolved inlets.
        ```
    """
    if source_path := kwargs.get('source_path'):
        if isinstance(source_path, str):
            from airflow.datasets import Dataset
            from cores.models.lineage import get_source_tables
            tables = get_source_tables(sql=source_path)
            kwargs["inlets"] = [Dataset(uri=m) for m in tables if m]

    return kwargs


def get_outlets(**kwargs):
    """
    Automatically resolve outlets for a task based on the sink path.

    Args:
        kwargs (dict): Key-value arguments to process.

    Returns:
        dict: Updated kwargs with resolved outlets.

    Examples:
        ```python
        kwargs = {"sink_path": "my_dataset"}
        updated_kwargs = get_outlets(**kwargs)
        print(updated_kwargs["outlets"])  # Outputs the resolved outlets.
        ```
    """

    from airflow.datasets import Dataset

    if sink_path := kwargs.get("sink_path"):
        if "/" not in sink_path:
            kwargs["outlets"] = Dataset(uri=sink_path)

    return kwargs
