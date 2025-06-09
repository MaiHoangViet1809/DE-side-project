from httpx import Client
import pandas as pd
from tabulate import tabulate
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from json import dumps
from contextlib import contextmanager, closing
from cores.utils.configs import FrameworkConfigs


class AirflowHook:
    """
    This hook support control airflow via rest api
    """

    def __init__(self,
                 base_url: str = FrameworkConfigs.Hooks.Airflow.BASE_URL,
                 auth=FrameworkConfigs.Hooks.Airflow.DEFAULT_AUTH,
                 headers=FrameworkConfigs.Hooks.Airflow.DEFAULT_HEADERS):
        self.base_url = base_url
        self.auth = auth
        self.headers = headers

    @property
    def create_client(self):
        return Client(headers=self.headers, auth=self.auth, base_url=self.base_url, verify=False)

    def get_dag_runs(self, dag_id: str, **kwargs):
        url = f"/api/v1/dags/{dag_id}/dagRuns"
        query_params = "&".join([f"{k}={v}" for k, v in kwargs.items()])
        with closing(self.create_client) as client:
            response = client.get(url + "?" + query_params)
            if response.status_code == 200:
                df = (pd.DataFrame(response.json()["dag_runs"])
                      .drop(["last_scheduling_decision", "execution_date", "run_type", "note", "data_interval_start", "data_interval_end"], axis="columns"))
                list_convert_dt = ["start_date", "end_date", "logical_date"]
                df[list_convert_dt] = df[list_convert_dt].astype("datetime64[ns]").apply(lambda x: x.dt.floor('S'))
                df = df[["dag_id", "dag_run_id", "logical_date", "state", "external_trigger", "start_date", "end_date", "conf"]]
                print(tabulate(df, headers="keys", tablefmt="psql"))
                return df
            else:
                print("ERROR:", response.text)

    def trigger_dag_run(self, dag_id: str, logical_date: str = None, dag_run_id: str = None, **conf):
        url = f"/api/v1/dags/{dag_id}/dagRuns"
        payload = {
            "dag_run_id": dag_run_id or f"manual__{uuid4()}",
            "logical_date": logical_date or (datetime.now(timezone.utc) + timedelta(hours=7) + timedelta(days=-1)).isoformat(),
            "conf": conf,
        }
        with closing(self.create_client) as client:
            response = client.post(url, data=dumps(payload))
            if response.status_code == 200:
                df = pd.DataFrame(response.json())
                df = df.drop(["last_scheduling_decision", "execution_date", "data_interval_start", "data_interval_end"], axis="columns")
                # list_convert_dt = ["start_date", "end_date", "logical_date"]
                # df[list_convert_dt] = df[list_convert_dt].astype("datetime64[ns]").apply(lambda x: x.dt.floor('S'))
                print(tabulate(df, headers="keys", tablefmt="psql"))
            else:
                print("ERROR:", response.text)


if __name__ == "__main__":
    # Unit test:
    hook = AirflowHook()

    hook.trigger_dag_run(dag_id="AIRFLOW-LOG-CLEAN-AUTO", logical_date="2023-12-17T10:29:00+07:00", test_conf_1=123)

    hook.get_dag_runs(dag_id="AIRFLOW-LOG-CLEAN-AUTO", limit=5, execution_date_gte="2023-12-16T00:00:00+07:00")