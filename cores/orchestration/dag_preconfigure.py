from pathlib import Path


def handle_configure(version: str = None, file_name: str = None, **configure):
    """
    Handle DAG configuration by adding Jinja2 macros, filters, and additional settings.

    Args:
        version (str, optional): Version of the DAG.
        file_name (str, optional): File name of the DAG.
        **configure: Additional DAG configuration parameters.

    Returns:
        dict: Updated configuration dictionary.

    Examples:
        ```python
        config = handle_configure(
            version="1.0.0",
            file_name="/path/to/dag.py",
            schedule="0 12 * * *",
            dag_id="example_dag"
        )
        print(config)
        ```
    """
    from cores.utils.datetime_macros import macro_format_date, macro_format_date_path, relativedelta, macro_get_run_date, strptime, months, days
    # auto tagging + schedule tagging
    schedule = configure.get("schedule", None)
    if isinstance(schedule, list):
        freq = "DATASET-TRIGGER-BASE"
    else:
        if schedule:
            schedule = schedule.split(" ")
        freq = next((i for i in range(len(schedule or [])) if schedule[i] == "*"), None)
        freq = {2: "DAILY", 3: "MONTHLY", None: "NONE"}.get(freq, "COMPLEX")
        if schedule:
            if schedule[-1] != "*":
                freq = "WEEKLY"

    addition_tags = [freq]

    if file_name:
        parent_folder = Path(file_name).parent.name.upper()
        addition_tags += [parent_folder]

    if version:
        addition_tags += [version]
    configure["tags"] = configure.get("tags", []) + addition_tags

    # macro
    macro = configure.get("user_defined_macros", {})
    macro["f_date_path"] = macro_format_date_path
    macro["f_date"] = macro_format_date
    macro["relativedelta"] = relativedelta
    macro["macro_get_run_date"] = macro_get_run_date
    macro["strptime"] = strptime
    macro["months"] = months
    macro["days"] = days
    configure["user_defined_macros"] = macro

    # macro filters
    filters = configure.get("user_defined_filters", {})
    filters["f_date_path"] = macro_format_date_path
    filters["f_date"] = macro_format_date
    filters["strptime"] = strptime
    configure["user_defined_filters"] = filters

    # param
    params = configure.get("params", {})
    params["job_freq"] = freq
    configure["params"] = params

    # dag_id always upper
    configure["dag_id"] = str(configure.get("dag_id")).upper()

    # default max_active_tasks and max_active_runs
    configure["max_active_runs"] = configure.get("max_active_runs", 1)
    configure["max_active_tasks"] = configure.get("max_active_tasks", 4)

    return configure


if __name__ == "__main__":

    print(Path(__file__).parent.name)