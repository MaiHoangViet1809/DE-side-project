from pathlib import Path

from cores.utils.env_info import Env


class MeasureManager:
    target_file_ext = ".sql"

    def __init__(self, sub_path: str):
        self._airflow_home = None
        self.sub_path = sub_path

    @property
    def base_path(self):
        return Path(self.get_env_airflow_home) / Path(self.sub_path)

    @property
    def get_env_airflow_home(self):
        if not self._airflow_home:
            self._airflow_home = Env.airflow_home()
        return self._airflow_home

    @get_env_airflow_home.setter
    def get_env_airflow_home(self, value):
        self._airflow_home = value

    def _render_path(self, filename: str):
        if not filename.lower().endswith(self.target_file_ext):
            filename += self.target_file_ext
        return self.base_path / Path(filename)
