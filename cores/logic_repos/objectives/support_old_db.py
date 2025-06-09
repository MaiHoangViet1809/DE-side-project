from pathlib import Path

from cores.hooks.sftp import SFTPHook, progress_callback
from cores.utils.providers import ProvideHook
from cores.utils.configs import FrameworkConfigs as cfg

from cores.logic_repos.objectives.spark import spark_ingestion, lit
from cores.models.refresh_type import IngestType
from cores.utils.shells import run_sh


@ProvideHook(cfg.Hooks.MSSQL.production, database="EDW")
def sftp_ingestion_to_old_db(parent_directory: str,
                             file_name_pattern: str,
                             sink_path: str,
                             **kwargs):
    """
    Ingests new files from an SFTP server into an old database, with optional transformations.

    Args:
        parent_directory (str): The parent directory on the SFTP server.
        file_name_pattern (str): A pattern to match filenames on the SFTP server.
        sink_path (str): The database path where the data will be ingested.
        **kwargs: Additional arguments, including:
            - hook: The database connection hook.

    Process:
        - Queries the database for files not yet imported.
        - Connects to the SFTP server and downloads new files.
        - Uses Spark to ingest files into the database with optional transformations.
        - Logs metadata such as file size, row count, and ingestion status to the database.

    Example:
        sftp_ingestion_to_old_db(
            parent_directory="/data/files",
            file_name_pattern="*.csv",
            sink_path="my_database.my_table"
        )
    """
    hook = kwargs.get("hook")
    sftp_hook = SFTPHook("sftp_default")

    def _download_file(full_path: str, sftp_client) -> Path:
        """
        Downloads a file from the SFTP server or retrieves it from the archive if it exists.

        Args:
            full_path (str): The full remote path to the file.
            sftp_client: The SFTP client instance.

        Returns:
            Path: The local path to the downloaded or archived file.
        """
        print(f"[_download_file] loading file: {Path(full_path).name}")

        # return archive file if exist (instead download again)
        base_folder = cfg.Ingestion.ONEDRIVE_BACKUP_LOCATION
        archive_file = Path(base_folder) / Path(full_path).name
        if archive_file.exists():
            return archive_file

        # download new file
        temp_path = f"/tmp/{Path(full_path).name}"
        sftp_client.get(remotepath=full_path, localpath=temp_path, callback=progress_callback)
        return Path(temp_path)

    print("[sftp_ingestion_to_old_db] start load list new files from sftp connection")
    sql_get_list_new_file = f"""
    SELECT T.FILENAME, T.FILE_SIZE
      FROM (SELECT DISTINCT FILENAME, FILE_SIZE
              FROM EDW.CONFIG.TBL_SFTP_FILE_LOG AS T
             WHERE T.FILENAME LIKE '%{file_name_pattern}%'
             )  T
        LEFT JOIN (SELECT FILE_NAMES
                     FROM [07.DATA_DMS].[dbo].[sFTP_IMPORT_FILE_KCVNBIZ] B
                    WHERE B.STATUS = 'Success'
                      AND B.FILE_NAMES LIKE '%{file_name_pattern}%'
                   ) B
               ON T.FILENAME = B.FILENAME
      WHERE B.FILE_NAMES IS NULL
    """
    df = hook.get_pandas_df(sql=sql_get_list_new_file)
    list_new_files = df["FILENAME"].tolist()

    with sftp_hook as client:
        for m in list_new_files:
            remote_path = Path(parent_directory.removesuffix("/")) / Path(m)
            temp_file = _download_file(full_path=str(remote_path), sftp_client=client)

            print("[sftp_ingestion_to_old_db] run ingestion in spark")
            spark_ingestion(source_path=str(temp_file),
                            source_format=temp_file.suffix.lower().removeprefix("."),
                            sink_path=sink_path,
                            sink_format="mssql",
                            file_name=remote_path.name,
                            write_mode="append",
                            hook_sink=hook,
                            transform_callables=[
                                lambda df: df.withColumns(dict(
                                    FILENAME=lit(remote_path.name)
                                ))
                            ],
                            ingest_type=IngestType.cdc,
                            **kwargs
                            )

            print("[sftp_ingestion_to_old_db] calculate file_size and row count of sftp file")
            FILE_SIZE = df[df["FILENAME"] == m]["FILE_SIZE"].values[0]
            rt_code, ROW_COUNT = run_sh(f"wc -l {temp_file} | awk '{{print $1}}'", stream_stdout=False)

            print("[sftp_ingestion_to_old_db] write log to DB")
            SQL_TO_LOG = f"""
            INSERT INTO [07.DATA_DMS].[dbo].[sFTP_IMPORT_FILE_KCVNBIZ] -- cần kiểm tra kỹ
            SELECT NULL                   AS [File_Date]
                   ,'{FILE_SIZE}'         AS [File_Size]
                   ,'{remote_path.name}'  AS [File_Names]
                   ,{ROW_COUNT[0]}                     AS [File_Row]
                   ,GETDATE()             AS [Time_Import]
                   ,'Success'             AS [Status]
            """
            hook.run_sql(SQL_TO_LOG)




