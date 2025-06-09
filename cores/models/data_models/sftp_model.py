from sqlalchemy import Column, Integer, DATETIME, VARCHAR, BigInteger  # DATE
from sqlalchemy.ext.declarative import declarative_base
from pandas import DataFrame, to_datetime
from pathlib import Path

from cores.models.refresh_type import IngestType
from cores.models.data_models.data_model_creation import BehaviorStandardDataModel

from cores.hooks.sftp import SFTPHook
from cores.hooks.mssql import SQLServerHook

from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.providers import ProvideHookFramework
from cores.utils.debug import print_table
from cores.utils.shells import run_sh
from cores.utils.functions import human_readable_size


# declarative base class
_Base = declarative_base()


class SFTPFileSynchronize(_Base, BehaviorStandardDataModel):
    """
    A class for managing SFTP file synchronization and logging file metadata in a database.

    Inherits from:
        - `_Base`: Declarative base for SQLAlchemy ORM models.
        - `BehaviorStandardDataModel`: Provides standard behaviors for data models.

    Attributes:
        parent_path (Column): The parent directory path of the file.
        filename (Column): The name of the file.
        last_modified_dt (Column): The last modified timestamp of the file.
        file_size (Column): The size of the file as a human-readable string.
        st_size (Column): The size of the file in bytes.
        st_mtime (Column): The last modified time of the file as an integer.
    """

    __tablename__ = "TBL_SFTP_FILE_LOG"
    __table_args__ = {"schema": cfg.Core.SCHEMA_EDW_FRAMEWORK}

    parent_path      = Column(VARCHAR(255), primary_key=True)
    filename         = Column(VARCHAR(255),  primary_key=True)
    last_modified_dt = Column(DATETIME)
    file_size        = Column(VARCHAR(50))
    st_size          = Column(BigInteger)
    st_mtime         = Column(Integer, primary_key=True)

    @classmethod
    def read_file_stat(cls, file_path: str):
        """
        Reads file metadata such as size and last modification time using the `stat` shell command.

        Args:
            file_path (str): Path to the file.

        Returns:
            tuple: A tuple containing file name, size (in bytes), and last modification time.
        """
        rtcode, stdout = run_sh(f"""stat -c "%n|%s|%Y" -t "{file_path}" """)
        print(f"[read_file_stat] {rtcode=} {stdout=}")
        file_name, st_size, st_mtime  = str(stdout[0]).split("|")
        return file_name, int(st_size), st_mtime

    @classmethod
    @ProvideHookFramework
    def list_current_files(cls, parent_directory: str, hook, file_name_pattern: str = None) -> DataFrame:
        """
        Retrieves a list of files currently logged in the database.

        Args:
            parent_directory (str): The parent directory of the files.
            hook: A database connection hook.
            file_name_pattern (str): A pattern to filter filenames. Default is None.

        Returns:
            DataFrame: A DataFrame containing file metadata.
        """
        from sqlalchemy.sql.expression import select
        from pandas import read_sql

        stmt = select(cls).where(cls.parent_path == parent_directory)

        if file_name_pattern is None:
            file_name_pattern = ""

        if file_name_pattern and not any(m in file_name_pattern for m in ["^", "$", "[", "]", "+"]):
            stmt = stmt.where(cls.filename.like("%" + file_name_pattern + "%"))

        with hook as session:
            df = read_sql(stmt, con=session.bind)
            if any(m in file_name_pattern for m in ["^", "$", "[", "]", "+"]):
                df = df[df["filename"].str.contains(file_name_pattern, na=False, regex=True)]

        return df.reset_index(drop=True)

    def list_remote_files(self, parent_directory: str, file_name_pattern: str = None, sftp_hook=None) -> DataFrame:
        """
        Retrieves a list of files from the remote SFTP server.

        Args:
            parent_directory (str): The parent directory on the SFTP server.
            file_name_pattern (str): A pattern to filter filenames. Default is None.
            sftp_hook: An SFTP connection hook. Default is None.

        Returns:
            DataFrame: A DataFrame containing metadata for remote files.
        """
        if not sftp_hook:
            sftp_hook = SFTPHook()

        df_all_file = sftp_hook.get_dir_info(parent_directory, conn_id=sftp_hook.ssh_conn_id)
        df = df_all_file.copy(deep=True)
        if file_name_pattern:
            # filter file with extension and size > 0
            df = df[df["filename"].str.contains(".") & df["st_size"].notnull()]

            # filter with file name pattern regex enable
            df = df[df["filename"].str.contains(file_name_pattern, na=False, regex=True)]
            # print_table(df.sort_values("filename", ascending=False), 10)

        df = df[self.__table__.columns.keys()]

        # if remote file gone, search in archive
        print(f"[list_remote_files] {df.size=} {df_all_file.size=} {file_name_pattern=}")
        if df.size == 0:
            print(f"[list_remote_files] start process searching in archive folder")
            from cores.utils.configs import FrameworkConfigs as cfg
            path_archive = cfg.Ingestion.ONEDRIVE_BACKUP_LOCATION
            rtcode, list_files = run_sh(f"ls '{path_archive}/' | grep '{file_name_pattern}'")
            if list_files:
                output = []
                for f in list_files:
                    file_name, st_size, st_mtime = self.read_file_stat(str(Path(path_archive) / f))
                    output.append(dict(
                        parent_path=str(Path(file_name).parent),
                        filename=Path(file_name).name,
                        last_modified_dt=to_datetime(st_mtime, unit='s'),
                        file_size=human_readable_size(st_size),
                        st_size=st_size,
                        st_mtime=st_mtime,
                    ))
                df = DataFrame(output)

        return df.reset_index(drop=True)

    @staticmethod
    def get_file_imported(sink_path: str, hook: SQLServerHook):
        """
        Retrieves a list of filenames that have already been imported into the database.

        Args:
            sink_path (str): The schema and table name in the format "schema.table".
            hook (SQLServerHook): A database connection hook.

        Returns:
            list: A list of filenames that have been imported.
        """
        # Met
        print("[get_file_imported] get list of filename imported in old framework")
        schema, table_name = sink_path.split(".")
        df_schema = hook.get_table_scheme(table_name=table_name, schema=schema).reset_index()
        list_files_imported = []

        if len(df_schema[df_schema.COL_NAME.str.upper().eq("FILENAME")]) > 0:
            list_files_imported += hook.get_pandas_df(sql=f"SELECT DISTINCT FILENAME FROM {sink_path}")["FILENAME"].tolist()

        if len(df_schema[df_schema.COL_NAME.str.upper().eq("FILE_NAME")]) > 0:
            list_files_imported += hook.get_pandas_df(sql=f"SELECT DISTINCT FILE_NAME FROM {sink_path}")["FILE_NAME"].tolist()

        list_files_imported = list(set(list_files_imported))
        return list_files_imported

    def list_new_files(self, parent_directory: str, file_name_pattern: str = None, sftp_hook=None, **kwargs):
        """
        Identifies new files available for processing by comparing local and remote file lists.

        Args:
            parent_directory (str): The parent directory for files.
            file_name_pattern (str): A pattern to filter filenames. Default is None.
            sftp_hook: An SFTP connection hook. Default is None.
            **kwargs: Additional arguments, such as `sink_path` and `hook` for retrieving imported files.

        Returns:
            DataFrame: A DataFrame containing metadata for new files.
        """
        df_local = self.list_current_files(parent_directory, file_name_pattern=file_name_pattern)
        df_remote = self.list_remote_files(parent_directory, file_name_pattern=file_name_pattern, sftp_hook=sftp_hook)

        print("[list_new_files] local log table")
        print_table(df_local.sort_values(by=["filename"], ascending=[False]), 5)
        print("[list_new_files] remote list file")
        print_table(df_remote.sort_values(by=["filename"], ascending=[False]), 5)

        list_files_imported = []
        if "sink_path" in kwargs and "hook" in kwargs:
            list_files_imported = self.get_file_imported(sink_path=kwargs.get("sink_path"), hook=kwargs.get("hook"))
            print(f"[list_new_files] num of file imported: {len(list_files_imported)=}")

        if len(df_local) == 0:
            df_local["last_modified_dt"] = df_local["last_modified_dt"].astype("datetime64[ns]")

        force_rerun = kwargs.get("force_rerun", False)
        if force_rerun:
            print(f"[list_new_files] {force_rerun=}")
            list_files_imported = list(set(list_files_imported) - {file_name_pattern})
            df_local = df_local[~df_local["filename"].str.startswith(file_name_pattern)]

        df_local = df_local.assign(INDEX_KEY=lambda x: x.parent_path + x.filename + x.last_modified_dt.dt.strftime('%Y%m%d%H%M%S'))
        df_remote = df_remote.assign(INDEX_KEY=lambda x: x.parent_path + x.filename + x.last_modified_dt.dt.strftime('%Y%m%d%H%M%S'))
        key_diff = set(df_remote.INDEX_KEY).difference(df_local.INDEX_KEY)
        print(f"{key_diff=}")
        print(f"{list_files_imported=}")

        # condition to filter
        if kwargs.get("ingest_type") == IngestType.masterdata:
            if list_files_imported:
                cond_filter = df_remote["filename"].gt(list_files_imported[0])
            else:
                latest_file = (
                    df_remote.sort_values(by=["filename"], ascending=[False])["filename"].tolist()[0]
                )
                cond_filter = df_remote["filename"].eq(latest_file)
        else:
            cond_filter = ~df_remote["filename"].isin(list_files_imported)

        df_output  = (
            df_remote[df_remote.INDEX_KEY.isin(key_diff)]
            [cond_filter]    # remove file already imported of init batches (from old db)
            .drop(columns=["INDEX_KEY"])
            .assign(full_path=lambda x: x.parent_path + "/" + x.filename)
            .sort_values(by=["filename"], ascending=[False])
        )
        if kwargs.get("ingest_type") == IngestType.masterdata:
            df_output = df_output.head(1)

        print(f"[list_new_files] {len(df_output)=}")
        return df_output

    @staticmethod
    def backup_raw_file(source_file: Path):
        """
        Backs up a raw file to a designated backup location.

        Args:
            source_file (Path): The path to the source file to be backed up.

        Process:
            - Copies the file to the backup directory specified in the configuration.
        """
        from shutil import copyfile
        from cores.utils.configs import FrameworkConfigs as FCG

        dest_path = Path(FCG.Ingestion.ONEDRIVE_BACKUP_LOCATION) / Path(source_file.stem + source_file.suffix)
        copyfile(source_file, dst=dest_path)


if __name__ == "__main__":
    # unitest
    test = SFTPFileSynchronize()
    ms_hook = SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME, schema=cfg.Core.SCHEMA_EDW_FRAMEWORK)

    new_files = test.list_new_files(parent_directory="VN/KCVNBiz", file_name_pattern="VN15_CUST_LISTING_")
    print_table(new_files)
