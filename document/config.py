from os import getenv
from dotenv import load_dotenv

load_dotenv()

AIRFLOW_HOME = getenv('AIRFLOW_HOME')

DEFAULT_WWW = f"{AIRFLOW_HOME}/../document/www"
DEFAULT_LOCATION = f"{DEFAULT_WWW}/js/library"
DEFAULT_LOCATION_CSS = f"{DEFAULT_WWW}/js/css"
DEFAULT_HOST = "118.68.168.179"

DEFAULT_ROUTE_API = "/api"
DEFAULT_ROUTE_LIBRARY = f"{DEFAULT_ROUTE_API}/library"
DEFAULT_ROUTE_JS = f"{DEFAULT_ROUTE_API}/js"
DEFAULT_ROUTE_BASE = "/framework-document"
