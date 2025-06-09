import logging
import sys
from datetime import datetime
from cores.utils.env_info import is_in_docker, is_under_airflow


class LogMixin:
    """
    Base class for logging mixin, with convenient method msg to send data to logger.

    This class provides a flexible logging mechanism that can be mixed into other classes.
    It initializes a logger, allows setting the log level, and provides a method for logging messages.
    """
    def __init__(self):
        """
        Initialize the LogMixin.

        Sets up a logger with the class name, configures it to not propagate messages,
        clears any existing handlers, adds a StreamHandler to stdout, and sets the initial log level to INFO.
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.propagate = False
        if self.logger.hasHandlers():
            self.logger.handlers.clear()
        self.logger.addHandler(logging.StreamHandler(sys.stdout))
        self.log_level = "INFO"
        self.logger.setLevel(self.log_level)

    def set_log_level(self, log_level: str = "INFO"):
        """
        Set the log level for the logger.

        Args:
            log_level (str): The log level to set. Defaults to "INFO".
        """
        self.log_level = log_level

    def msg(self, msg, log_level=None, class_name=None):
        """
        Log a message with the specified log level and class name.

        If running in a Docker container or under Airflow, the message is printed to stdout.
        Otherwise, it's logged using the configured logger.

        Args:
            msg (str): The message to log.
            log_level (str, optional): The log level for this message. If None, uses the instance's log_level.
            class_name (str, optional): The class name to include in the log message. If None, uses the instance's class name.

        Returns:
            None
        """
        if not class_name:
            class_name = self.__class__.__name__
        template = f"[class={class_name}] {msg}"  # {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        if is_in_docker() or is_under_airflow():
            print(template)
        else:
            # f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"
            self.logger.log(msg=template, level=logging._nameToLevel[log_level or self.log_level])


if __name__ == "__main__":
    # Unit test
    class Test(LogMixin):
        def run_it(self):
            self.msg("yes run it please")
            return self

    Test().run_it()
