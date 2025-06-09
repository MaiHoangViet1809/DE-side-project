from cores.models.jinja import SQLRender, IRender, FileRender
from cores.orchestration.task_input_process import resolve_file
from os import environ
from jinja2.visitor import NodeVisitor
from jinja2 import Environment, nodes
import argparse


class FunctionCallVisitor(NodeVisitor):
    """
    A visitor class for extracting function calls from Jinja2 templates.

    Attributes:
        function_calls (list[str]): List of function call expressions found in the template.
        env (Environment): Jinja2 environment for parsing templates.
        parsed_template (Template): Parsed representation of the Jinja2 template.
    """

    def __init__(self, template: str, env = None):
        """
        Initializes the FunctionCallVisitor with a template and environment.

        Args:
            template (str): Jinja2 template as a string.
            env (Environment, optional): Jinja2 environment. Defaults to a new environment instance.
        """
        self.function_calls = []
        if not env: env = Environment()
        self.env = env
        self.parsed_template = env.parse(template)

    def visit_Call(self, node):
        """
        Visits nodes representing function calls in the Jinja2 template.

        Args:
            node (nodes.Node): A Jinja2 node representing a function call.
        """
        if isinstance(node.node, nodes.Name):
            function_name = node.node.name
            arguments = [f"'{arg.value}'" for arg in node.args]
            self.function_calls.append(f"{function_name}({', '.join(arguments)})")

    def resolve(self):
        """
        Resolves all function calls in the parsed template.

        Returns:
            list[str]: A list of string representations of function calls.
        """
        self.visit(self.parsed_template)
        return self.function_calls


def compile_template(sql_template: str, airflow_home: str = None, **variables):
    """
    Compiles a SQL template with the given variables and environment settings.

    Args:
        sql_template (str): Path to the SQL template file or the SQL template as a string.
        airflow_home (str, optional): Path to the Airflow home directory. Defaults to `None`.
        **variables: Key-value pairs representing variables to be rendered in the template.

    Returns:
        str: The compiled SQL template with all variables and macros resolved.
    """
    environ["AIRFLOW_HOME"] = "../../EDW-KC-Project/airflow_home"
    if not airflow_home:
        FileRender.get_env_airflow_home = airflow_home

    sql_render = SQLRender(**variables)
    framework_render = IRender({})

    print(sql_render.base_path)

    if sql_template.lower().endswith(".sql"):
        sql_template = resolve_file(source_path=sql_template)
    # print variables in template
    print("-" * 50)
    print(f"Parsed template: {sql_template=}")
    print("Variables macro SQL:", FunctionCallVisitor(sql_template).resolve())
    print("Variables framework:", framework_render.find_vars(sql_template))
    print("-" * 50)

    # print SQL:
    sql_template = sql_render.render(sql_template)
    if isinstance(sql_template, dict):
        sql_template = sql_template.get("source_path")
    sql_template = framework_render.render(sql_template, **variables)
    return sql_template


if __name__ == "__main__":
    from pathlib import Path
    path_to_airflow_home = str(Path(__file__).parent.parent / Path("airflow_home"))

    parser = argparse.ArgumentParser(description="cli to compile sql template")

    parser.add_argument("--file", "-F", "--filename", type=str, help="file sql to compile")
    parser.add_argument("--run_date", "-D", type=str, default=None, help="run_date")

    args = parser.parse_args()

    config_variables = dict(
        sql_template=Path(f"{path_to_airflow_home}/dags/sql/{args.file}").read_text(),
        run_date=args.run_date,
    )
    print(compile_template(airflow_home=path_to_airflow_home, **config_variables))
