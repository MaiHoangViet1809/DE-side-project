from enum import Enum


def dynamic_import(name: str | Enum):
    print(f"[dynamic_import] getting engine [{name}]")
    if isinstance(name, Enum):
        name = name.value

    from importlib import import_module
    from pathlib import Path
    from dotenv import load_dotenv
    from cores.utils.env_info import Env
    load_dotenv()
    framework_path = Env.framework_home()

    list_module_name = [str(m).replace(".py", "")
                        for m in Path(f"{framework_path}/cores/engines").glob("[!__init__]*.py")]

    for m in list_module_name:
        module = import_module(f"cores.engines.{m.split('/')[-1]}")
        engine = getattr(module, name, None)
        if engine:
            return engine


class Engine(Enum):
    ODBC = "ODBCEngine"
    SPARK = "SparkEngine"
    POLARS = "PolarsEngine"
    DASK = "DaskEngine"


__all__ = ['Engine', "dynamic_import"]


