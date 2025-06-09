from os.path import join
from pathlib import Path

from cores.hooks.mssql import SQLServerHook
from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.env_info import Env


def find_file_match_pattern(base_folder: str, pattern: str):
    """
    Finds files in a specified folder that match a given pattern.

    Args:
        base_folder (str): The base folder to search in.
        pattern (str): The file pattern to match (e.g., "*.sql").

    Returns:
        generator: A generator of `Path` objects for the matching files.

    Example:
        >>> for file in find_file_match_pattern("/path/to/folder", "*.sql"):
        ...     print(file)
    """
    return Path(join(base_folder)).glob(pattern)


def auto_update_view(relative_path: str, pattern: str, **kwargs):
    """
    Automatically updates SQL views based on files in a specified directory matching a pattern.

    Args:
        relative_path (str): The relative path to the directory containing view definition files.
        pattern (str): The file pattern to match (e.g., "*.sql").
        **kwargs: Additional arguments for customization.

    Process:
        - Searches for files matching the pattern in the specified relative path.
        - Reads the SQL definition from each file.
        - Executes a `CREATE OR ALTER VIEW` statement for each view using the SQL content.
        - Refreshes the views in the database by executing `sp_refreshview`.

    Example:
        auto_update_view(
            relative_path="sql/views",
            pattern="*.sql"
        )
    """
    hook = SQLServerHook(cfg.Hooks.MSSQL.new, database="EDW", schema="DM")
    airflow_home = Env.airflow_home()

    for view_def_path in find_file_match_pattern(base_folder=join(airflow_home, relative_path), pattern=pattern):
        sql = view_def_path.read_text()
        view_name = view_def_path.stem
        dml_sql = f"CREATE OR ALTER VIEW {view_name} AS {sql}"
        hook.run_sql(dml_sql)
        sql = f"exec sp_refreshview @viewname='{view_name}'"
        hook.run_sql(sql)


if __name__ == "__main__":
    auto_update_view(relative_path="dags/sql/", pattern="DM.VW_*.sql")
