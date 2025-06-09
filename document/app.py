import subprocess
import socket
import time
from uuid import uuid4
from pathlib import Path
from os import getenv
import requests

from flask import Blueprint, request, Response, current_app
from flask_appbuilder import expose, BaseView as AppBuilderBaseView
from airflow.plugins_manager import AirflowPlugin
from airflow.security import permissions
from airflow.www.auth import has_access

from document.config import (
    DEFAULT_ROUTE_LIBRARY,
    DEFAULT_ROUTE_JS,
    DEFAULT_ROUTE_API,
    DEFAULT_ROUTE_BASE,
    DEFAULT_WWW,
)
from cores.models.jinja import IRender
from cores.utils.providers import ProvideErrorLogging
from dotenv import load_dotenv

load_dotenv()
AIRFLOW_HOME = getenv("AIRFLOW_HOME")

bp = Blueprint("dp-document", __name__)


def is_mkdocs_running(host="127.0.0.1", port=8000) -> bool:
    """Check if host:port is listening."""
    import socket

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.5)
    try:
        s.connect((host, port))
        s.shutdown(socket.SHUT_RDWR)
        return True
    except:
        return False
    finally:
        s.close()


def launch_mkdocs(host="127.0.0.1", port=8000):
    """
    Spawn `mkdocs serve --dev-addr host:port` in background if not already running.
    """
    from subprocess import Popen, DEVNULL

    mkdocs_cmd = [
        "mkdocs",
        "serve",
        "--dev-addr",
        f"{host}:{port}",
    ]
    # Adjust working dir to your mkdocs.yml location:
    Popen(
        mkdocs_cmd,
        cwd=Path(AIRFLOW_HOME).parent / "document",  # or wherever mkdocs.yml is
        stdout=DEVNULL,
        stderr=DEVNULL,
    )
    # Wait up to 10 seconds for mkdocs to start
    for _ in range(10):
        time.sleep(1)
        if is_mkdocs_running(host, port):
            break


# -------------------------------------------------------------------
# Example existing Airflow-based view
# -------------------------------------------------------------------
class CustomView(AppBuilderBaseView):
    default_view = "get_home"
    route_base = DEFAULT_ROUTE_BASE

    @expose("/")
    @has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
    def get_home(self):
        return self.render_template(
            template=f"{AIRFLOW_HOME}/../document/home.html",
            route_api_library=DEFAULT_ROUTE_LIBRARY,
            route_api_js=DEFAULT_ROUTE_JS,
            __version=str(uuid4()),
        )


@bp.route(DEFAULT_ROUTE_API + "/js")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
@ProvideErrorLogging
def serve_js():
    from pathlib import Path

    js_name = request.args.get("js_name")
    file_path = Path(f"{DEFAULT_WWW}/js/{js_name}")
    if file_path.is_file():
        content = file_path.read_text()
    else:
        content = "// not found"
    res_text = IRender(dict(route_base=DEFAULT_ROUTE_BASE)).render(data=content)
    return Response(res_text, status=200, mimetype="application/javascript")


@bp.route(DEFAULT_ROUTE_API + "/library")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
@ProvideErrorLogging
def serve_library():
    from urllib.parse import unquote
    from document.js_libraries_downloader import synchronize_script, DEFAULT_LOCATION, DEFAULT_LOCATION_CSS

    url = request.args.get("url")
    url = unquote(url, encoding="utf-8", errors="replace")
    if "css" in url:
        file_content = synchronize_script(url, DEFAULT_LOCATION_CSS)
        return Response(file_content, status=200, mimetype="text/css")
    else:
        file_content = synchronize_script(url, DEFAULT_LOCATION)
        return Response(file_content, status=200, mimetype="application/javascript")


# -------------------------------------------------------------------
# NEW: Reverse-proxy route for mkdocs
# -------------------------------------------------------------------
@bp.route("/mkdocs/<path:subpath>")
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
def mkdocs_proxy(subpath):
    """
    Reverse proxy to local mkdocs server:
    e.g. GET /mkdocs/index.html -> internally fetch http://127.0.0.1:8000/index.html
    and return the result to the client.
    """
    return _mkdocs_proxy_impl(subpath)


@bp.route("/mkdocs", defaults={"subpath": ""})
@has_access([(permissions.ACTION_CAN_READ, permissions.RESOURCE_WEBSITE)])
def mkdocs_proxy_root(subpath):
    """
    If subpath is empty, effectively /mkdocs -> http://127.0.0.1:8000/
    """
    return _mkdocs_proxy_impl(subpath)


def _mkdocs_proxy_impl(subpath=""):
    mkdocs_url = f"http://127.0.0.1:8000/{subpath}"
    current_app.logger.info(f"[mkdocs_proxy] fetch -> {mkdocs_url}")

    # Pass along query params if needed:
    # e.g. mkdocs_url += "?"+ request.query_string.decode("utf-8")

    resp = requests.request(
        method=request.method,
        url=mkdocs_url,
        headers={k: v for k, v in request.headers if k.lower() not in ["host", "content-length"]},
        data=request.get_data(),
        cookies=request.cookies,
        allow_redirects=False,
        stream=True,
    )

    # Build a Flask Response
    excluded_headers = ["content-encoding", "transfer-encoding", "connection"]
    headers = []
    for name, value in resp.headers.items():
        if name.lower() not in excluded_headers:
            headers.append((name, value))

    response = Response(resp.content, resp.status_code, headers)
    return response


# -------------------------------------------------------------------
# Plugin registration
# -------------------------------------------------------------------
v_appbuilder_package = {
    "name": "Project Document",
    "category": "Extra Functions",
    "view": CustomView(),
}


class AirflowPluginDocumentApp(AirflowPlugin):
    name = "document-app-plugin"
    flask_blueprints = [bp]
    appbuilder_views = [v_appbuilder_package]

    @classmethod
    def on_load(cls, *args, **kwargs):
        """Called when Airflow loads the plugin."""
        host = "127.0.0.1"
        port = 8000
        if not is_mkdocs_running(host, port):
            current_app.logger.info("[document-app-plugin] Starting mkdocs on port 8000...")
            launch_mkdocs(host, port)
        else:
            current_app.logger.info("[document-app-plugin] mkdocs is already running.")