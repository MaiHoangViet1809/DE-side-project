from enum import Enum, EnumMeta
from pathlib import Path
from inspect import signature
from functools import partial
from typing import List, Dict, Callable

from jinja2 import Environment

from cores.utils.debug import print_table
from cores.utils.configs import FrameworkConfigs as CFG
from cores.utils.env_info import Env
from cores.utils.email_misc import list_of_dict_to_html


class IRender:
    """
    IRender is a utility base class designed for handling data rendering tasks
    using the Jinja2 templating engine. It manages macros, filters, and rendering
    logs while offering extensibility for subclasses to specialize rendering behavior.

    Attributes:
        _DEFAULT_FILTER: Default filters for the Jinja2 environment.
        _DEFAULT_MACRO: Default macros for the Jinja2 environment.
        _context: A dictionary storing contextual data for rendering.
        _env: Jinja2 environment configuration.
        _datetime_dict: Utility dictionary for datetime-related functions.
        _render_type_accept: List of acceptable types for rendering.
        _logs: A list to store rendering logs.
    """
    _DEFAULT_FILTER = CFG.Core.Jinja.DEFAULT_FILTER
    _DEFAULT_MACRO = CFG.Core.Jinja.DEFAULT_MACRO

    def __init__(self, context: dict):
        """
        Initializes the IRender class with context and sets up the Jinja2 environment.

        Args:
            context (dict): The context dictionary for rendering.
        """
        from datetime import datetime, timedelta
        from dateutil.relativedelta import relativedelta
        import cores.utils.datetime_macros as m
        self._datetime_dict = {
            "datetime": datetime,
            "timedelta": timedelta,
            "relativedelta": relativedelta,
            "strftime": datetime.strftime,
        }
        self._context = context
        self._module = m
        self._env = Environment(variable_start_string=CFG.Core.Jinja.VAR_START_STRING,
                                variable_end_string=CFG.Core.Jinja.VAR_END_STRING,
                                block_start_string="[%",
                                block_end_string="%]")
        self._env.filters.update({k: getattr(self, k) for k in self._DEFAULT_FILTER})
        self._default_macros = {k if isinstance(k, str) else k.__name__
                                : getattr(self, k) if isinstance(k, str) else k
                                for k in self._DEFAULT_MACRO}
        self._render_type_accept = [str, dict, list, tuple, Callable]
        self._logs: List[Dict] = []

        self.add_macro("ref", load_sql)
        self.add_macro("load_sql", load_sql)
        self.add_macro("load_table_from_dicts", list_of_dict_to_html)

    def add_macro(self, name, callback: Callable):
        """
        Adds a macro to the default macro list.

        Args:
            name (str): The name of the macro.
            callback (Callable): The callback function for the macro.
        """
        if "**" in str(signature(callback)):
            callback = partial(callback, **self._context)
        self._default_macros[name] = callback

    def render(self, data, depth: int = 0, key: str = None, **kwargs):
        """
        Renders data recursively based on its type.

        Args:
            data: The data to render.
            depth (int): The recursion depth for rendering.
            key (str): The key for logging (optional).
            **kwargs: Additional rendering context.

        Returns:
            Rendered data.
        """
        from attrs import has

        try:
            if isinstance(data, dict):
                return {k: (self.render(v, depth + 1, key=k, **kwargs) if isinstance(v, tuple(self._render_type_accept)) else v) for k, v in data.items()}
            elif isinstance(data, (list, tuple)):
                return [self.render(m, depth + 1, key=key, **kwargs) if isinstance(m, tuple(self._render_type_accept)) else m for m in data]
            elif isinstance(data, Callable):
                if "**" in str(signature(data)):
                    return partial(data, **self._context)
                return data
            elif isinstance(data, (type, Enum, EnumMeta)) or has(data.__class__):
                return data
            # elif isinstance(data, BaseConnector):
            #     data.data = self._render(data.data, depth, key, **kwargs) if data.data else data.data
            #     return data
            elif isinstance(data, tuple(self._render_type_accept)):
                if isinstance(data, (dict, list, tuple)):
                    return self.render(data, depth, key, **kwargs)

                else:
                    return self._render(data, depth, key, **kwargs)

            # return raw for all other type
            else:
                return data
        except Exception as e:
            print(f"[{self.__class__.__name__}] render:: ERROR::", data)
            raise e

    def print_logs(self):
        """
         Prints rendering logs in a tabular format.
         """
        from pandas import DataFrame
        print(f"[{self.__class__.__name__}.print_logs] context: {self._context=}")
        print_table(DataFrame(self._logs))

    def _render(self, value, depth: int = 0, key: str = None, **kwargs):
        """
        Handles rendering of individual values.

        Args:
            value: The value to render.
            depth (int): The recursion depth for rendering.
            key (str): The key for logging (optional).
            **kwargs: Additional rendering context.

        Returns:
            Rendered value.
        """
        if isinstance(value, bool):
            rendered_data = value
        else:
            rendered_data = (self._env
                             .from_string(source=value)
                             .render(**self._default_macros, **self._context, **kwargs)
                             )
        self._logs += [dict(depth=depth, key=key, data_type=str(type(value)), data=rendered_data)]
        return rendered_data

    def __getattr__(self, item: str):
        """
        Dynamically resolves attributes for datetime utilities and macros.

        Args:
            item (str): The attribute name.

        Returns:
            The resolved attribute.
        """
        if item in self._datetime_dict:
            return self._datetime_dict[item]
        elif item in set(self._DEFAULT_MACRO + self._DEFAULT_FILTER):
            return getattr(self._module, item)
        raise AttributeError(f"{self.__class__.__name__}::{item} is not exist")

    def find_vars(self, value):
        """
        Finds undeclared variables in a template.

        Args:
            value: The template string.

        Returns:
            List of undeclared variables.
        """
        from jinja2.meta import find_undeclared_variables
        return list(find_undeclared_variables(self._env.parse(value)))


class FileRender(IRender):
    """
    FileRender extends IRender to support rendering tasks for file-based templates,
    with additional properties and methods specific to file handling.

    Attributes:
        sub_path (str): Subdirectory path for templates.
        target_file_ext (str): Default file extension for target files.
        _airflow_home (str): Stores the Airflow home directory path (initialized as None).
    """
    sub_path = "dags/sql/macro_template"
    target_file_ext = ".sql"

    @property
    def base_path(self):
        """
        Returns the base path for templates.

        Constructs the base path using the Airflow home directory and sub_path.

        Returns:
            Path: The base path for templates.
        """
        return Path(self.get_env_airflow_home) / Path(self.sub_path)

    def __init__(self, **kwargs):
        """
        Initializes the class with additional file-specific configurations.

        Args:
            **kwargs: Contextual parameters for rendering.
        """
        super().__init__(context=kwargs)
        self._airflow_home = None
        self._env = Environment(variable_start_string="{{",
                                variable_end_string="}}",
                                block_start_string="{%",
                                block_end_string="%}")

    def _render_path(self, filename: str):
        """
        Constructs the full file path for a given filename.

        Appends the target file extension if not already present.

        Args:
            filename (str): The filename to resolve.

        Returns:
            Path: The resolved file path.
        """
        if not filename.lower().endswith(self.target_file_ext):
            filename += self.target_file_ext
        return self.base_path / Path(filename)

    def _load_content(self, filename: str):
        """
        Loads the content of a specified file.

        Reads file content from the resolved path or returns the filename
        if the file doesn't exist.

        Args:
            filename (str): The file to load content from.

        Returns:
            str: The loaded content or the original filename if not found.
        """
        if self._render_path(filename=filename).exists():
            return self._render_path(filename=filename).read_text()
        return filename

    def render_file(self, filename):
        """
        Renders the content of a file and resolves variables.

        Finds and replaces variables in the file content using Jinja2 rendering.

        Args:
            filename (str): The file to render.

        Returns:
            str: Rendered file content.
        """
        new_data = self._load_content(filename)
        variables = self.find_vars(new_data)
        if len(variables) > 0:
            return super().render(new_data)
        else:
            return new_data

    def render(self, data, depth: int = 0, key: str = None, **kwargs):
        """
        Overrides IRender.render for file-specific rendering behavior.

        Ensures exceptions are handled and logged during rendering.

        Args:
            data: The data to render.
            depth (int): The recursion depth for rendering.
            key (str): Optional key for logging.
            **kwargs: Additional rendering context.

        Returns:
            Rendered data.
        """
        try:
            return super().render(data, **kwargs)
        except Exception as e:
            print(f"[{self.__class__.__name__}] render:: ERROR::", data)
            raise e

    @property
    def get_env_airflow_home(self):
        """
        Property to retrieve the Airflow home environment path.

        Lazily initializes the Airflow home path using `Env.airflow_home()`
        if not already set.

        Returns:
            str: The Airflow home path.
        """
        if not self._airflow_home:
            self._airflow_home = Env.airflow_home()
        return self._airflow_home

    @get_env_airflow_home.setter
    def get_env_airflow_home(self, value):
        """
        Setter to update the Airflow home environment path.

        Args:
            value (str): The new Airflow home path.
        """
        self._airflow_home = value


def load_sql(filename: str, **kwargs):
    """
    Loads and renders SQL templates from files using either SQLRender or
    SQLRenderExtend based on the file path or a root flag.

    Args:
        filename (str): Name of the SQL file to load.
        **kwargs: Additional context parameters for rendering.

    Returns:
        str: Rendered SQL content.
    """
    # print(f"data: {filename=} {kwargs=}")
    is_root = kwargs.pop('is_root', False)
    if "/" in filename or is_root:
        return SQLRenderExtend(**kwargs).render_file(filename)
    return SQLRender(**kwargs).render_file(filename)


class SQLRender(FileRender):
    """
    SQLRender is a subclass of FileRender specialized for rendering SQL templates.

    Attributes:
        sub_path: Subdirectory path for SQL templates.
        target_file_ext: Default file extension for SQL files.
    """
    sub_path = "dags/sql/macro_template"
    target_file_ext = ".sql"


class SQLRenderExtend(FileRender):
    """
    SQLRenderExtend extends FileRender with a different subdirectory path
    for SQL templates.

    Attributes:
        sub_path: Subdirectory path for extended SQL templates.
        target_file_ext: Default file extension for SQL files.
    """
    sub_path = "dags/sql"
    target_file_ext = ".sql"


class EmailRender(FileRender):
    """
    EmailRender extends FileRender to support rendering email templates,
    adding specific macros and modifying content loading behavior.

    Attributes:
        sub_path: Subdirectory path for email templates.
        target_file_ext: Default file extension for email templates.
    """
    sub_path = "dags/template/email"
    target_file_ext = ".template"

    def __init__(self, **context):
        """
        Initializes the EmailRender class with email-specific configurations
        and adds macros for table loading.

        Args:
            **context: Contextual parameters for rendering.
        """
        super().__init__(**context)
        self.add_macro("load_table", self.load_table_html)

    @staticmethod
    def load_table_html(sql: str, n_rows: int = 100):
        """
        Loads an SQL query as an HTML table.

        Args:
            sql (str): The SQL query to execute.
            n_rows (int): Number of rows to fetch and render as an HTML table.

        Returns:
            str: HTML representation of the SQL query result.
        """
        from cores.hooks.mssql import SQLServerHook

        hook = SQLServerHook(CFG.Hooks.MSSQL.new)
        if " " in sql: sql = f"({sql}) T"
        df = hook.get_pandas_df(f"SELECT TOP {n_rows} * FROM {sql}").head(n_rows)

        return list_of_dict_to_html(df.to_dict('records'))

    def _load_content(self, filename: str):
        """
        Overrides the content loader to format data for email rendering.

        Args:
            filename (str): The file to load content from.

        Returns:
            str: Formatted file content.
        """
        data = super()._load_content(filename)
        data = data.replace("\n", "<br>\n")
        return data


if __name__ == "__main__":
    # unit tests
    # j = SQLRender()
    # j.get_env_airflow_home = "/Users/maihoangviet/Projects/EDW-KC-Project/airflow_home"
    # print(j.render("""SELECT * FROM ({{ ref('INVOICE_HDR_MTD') }}) ABCD [[ run_date ]] =={{ run_date }}+++"""))

    e = EmailRender(run_date="2022-02-02")
    # e.get_env_airflow_home = "/Users/maihoangviet/Projects/EDW-KC-Project/airflow_home"
    print(e.render_file("test_email.template"))
