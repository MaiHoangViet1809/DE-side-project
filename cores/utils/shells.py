from subprocess import Popen, PIPE, STDOUT
from cores.utils.env_info import get_platform


def run_sh(command, stream_stdout=True, return_log=True, executable: str | None = None):
    """
    Run a shell/bash command using a subprocess.

    Args:
        command (str): The shell command to execute.
        stream_stdout (bool, optional): Whether to print stdout lines in real time.
        return_log (bool, optional): Whether to capture and return stdout lines.
        executable (str | None, optional): Path to the shell executable to use.
            If None on Linux, defaults to "/bin/bash".

    Returns:
        int | tuple[int, list[str]]:
            - If `return_log` is False, returns only the exit code (int).
            - If `return_log` is True, returns a tuple containing:
                - Exit code (int)
                - List of logged output lines (list[str])

    Examples:
        ```python
        # Simple usage: capture exit code and output
        exit_code, output = run_sh("echo Hello World")
        print("Exit code:", exit_code)
        print("Output:", output)

        # Suppress stdout streaming but still capture logs
        exit_code, output = run_sh("echo Silent World", stream_stdout=False)
        print("Exit code:", exit_code)
        print("Output:", output)

        # Run command and only return exit code
        code_only = run_sh("echo No Logs", return_log=False)
        print("Exit code:", code_only)
        ```
    """
    platform = get_platform()
    if platform == "linux":
        executable = "/bin/bash"

    full_log = []
    process = None
    try:
        process = Popen(command,
                        shell=True,
                        stdout=PIPE,
                        stderr=STDOUT,
                        encoding='utf-8',
                        errors='replace',
                        executable=executable)
        while True:
            realtime_output = process.stdout.readline()
            if realtime_output == '' and process.poll() is not None:
                break
            else:
                line_log = realtime_output.strip()
                if stream_stdout: print(line_log)
                if return_log: full_log.append(line_log)
    finally:
        process.kill()
        if return_log:
            return process.returncode, full_log
        else:
            return process.returncode
