import os


def is_in_docker() -> bool:
    """
    Check if the current process is running inside a Docker container.

    Returns:
        bool: True if running in Docker, False otherwise.

    Examples:
        ```python
        if is_in_docker():
            print("Running inside Docker.")
        else:
            print("Not running inside Docker.")
        ```
    """
    path = '/proc/self/cgroup'
    if os.path.exists('/.dockerenv'):
        return True
    elif os.path.isfile(path):
        with open(path) as f:
            return any('docker' in line for line in f)
    else:
        return False


def has_java_home():
    """
    Check if the host system has Java installed by verifying the `JAVA_HOME` environment variable.

    Returns:
        bool: True if `JAVA_HOME` is set, False otherwise.

    Examples:
        ```python
        if has_java_home():
            print("JAVA_HOME is set.")
        else:
            print("JAVA_HOME is not set.")
        ```
    """
    return os.getenv('JAVA_HOME', None) is not None


def get_platform():
    """
    Get the platform/operating system of the host.

    Returns:
        str: The platform name. Possible values are:
             - "linux" for Linux systems
             - "mac" for macOS
             - "windows" for Windows
             - "others" for unknown platforms

    Examples:
        ```python
        platform = get_platform()
        print(f"Running on: {platform}")
        ```
    """
    from sys import platform
    return {
        "linux": "linux",
        "linux2": "linux",
        "darwin": "mac",
        "win32": "windows",
    }.get(platform, "others")


def set_java_home():
    """
    Automatically set the `JAVA_HOME` environment variable based on the host platform.

    Returns:
        None

    Examples:
        ```python
        set_java_home()
        print(os.getenv("JAVA_HOME"))
        ```
    """
    from cores.utils.shells import run_sh
    match get_platform():
        case "linux":
            _, java_path = run_sh("echo /usr/lib/jvm/java-1.8*")
            java_path = java_path[0]
        case "mac":
            _, java_fullpath = run_sh("/usr/libexec/java_home")
            split_pos = str(java_fullpath[0]).find("openjdk") + len("openjdk")
            java_path = java_fullpath[0][:split_pos]
        case "windows":
            _, java_fullpath = run_sh("where java", executable=None)
            filter_list = [m for m in java_fullpath if "jdk1.8" in m]
            java_path = str(filter_list[0]).removesuffix(r"\bin\java.exe")
        case _:
            java_path = ""
    os.environ["JAVA_HOME"] = java_path


def is_under_airflow():
    """
    Check if the current process is running under an Airflow worker.

    Returns:
        bool: True if the process is running under Airflow, False otherwise.

    Examples:
        ```python
        if is_under_airflow():
            print("Running under Airflow.")
        else:
            print("Not running under Airflow.")
        ```
    """
    from inspect import stack
    return any("airflow" in s.filename for s in stack())


class Env:
    """
    The `Env` class provides access to environment variables.

    Methods:
        airflow_home():
            Get the value of the `AIRFLOW_HOME` environment variable.

        framework_home():
            Get the value of the `FRAMEWORK_HOME` environment variable.
    """
    @staticmethod
    def airflow_home():
        """
        Get the value of the `AIRFLOW_HOME` environment variable.

        Returns:
            str | None: The value of `AIRFLOW_HOME`, or None if not set.

        Examples:
            ```python
            airflow_path = Env.airflow_home()
            print(f"AIRFLOW_HOME: {airflow_path}")
            ```
        """
        return os.getenv("AIRFLOW_HOME")

    @staticmethod
    def framework_home():
        """
        Get the value of the `FRAMEWORK_HOME` environment variable.

        Returns:
            str | None: The value of `FRAMEWORK_HOME`, or None if not set.

        Examples:
            ```python
            framework_path = Env.framework_home()
            print(f"FRAMEWORK_HOME: {framework_path}")
            ```
        """
        return os.getenv("FRAMEWORK_HOME")


def get_env(key: str, cwd: str = __file__):
    """
    Retrieve the value of an environment variable from a `.env` file in the specified working directory.

    Args:
        key (str): The environment variable key to retrieve.
        cwd (str, optional): The current working directory. Defaults to `__file__`.

    Returns:
        str: The value of the environment variable.

    Examples:
        ```python
        value = get_env("MY_ENV_VAR", cwd="/path/to/project")
        print(f"Value: {value}")
        ```
    """
    from dotenv import get_key, find_dotenv
    from os import chdir
    from pathlib import Path
    chdir(Path(cwd).parent.resolve())
    env_path = find_dotenv(usecwd=True)
    return get_key(env_path, key)
