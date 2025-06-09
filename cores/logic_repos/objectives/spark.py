import os
from contextlib import contextmanager

from cores.hooks.mssql import SQLServerHook
from cores.hooks.sftp import SFTPHook, progress_callback

from cores.engines.spark_engine import SparkEngine

from cores.models.refresh_type import RefreshType, IngestType
from cores.models.data_models.sftp_model import SFTPFileSynchronize
from cores.models.data_models.onedrive_model import OneDriveFileSynchronize

from cores.utils.debug import print_table
from cores.utils.configs import FrameworkConfigs as cfg
from cores.utils.providers import ProvideBenchmark, ProvideETLLogging, ProvideDefault

from pyspark.sql.functions import *
from datetime import datetime
from pathlib import Path
from typing import Callable, List
from os.path import join
from glob import glob

@ProvideBenchmark
@ProvideDefault(
    hook_source=SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
    hook_sink=SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
    refresh_type=RefreshType.partition_mtd,
)
@ProvideETLLogging
def spark_transformation(source_path: str,
                         sink_path: str,

                         refresh_type: RefreshType = None,

                         source_format: str = "mssql",
                         sink_format: str = "mssql",

                         hook_source: SQLServerHook = None,
                         hook_sink: SQLServerHook = None,

                         transform_callables: List[Callable] = None,
                         **kwargs):
    """
    Executes a Spark transformation process from a source to a sink with optional transformations.

    Args:
        source_path (str): The path to the source data.
        sink_path (str): The path to the sink data.
        refresh_type (RefreshType, optional): The type of refresh to perform. Default is None.
        source_format (str): The format of the source data. Default is "mssql".
        sink_format (str): The format of the sink data. Default is "mssql".
        hook_source (SQLServerHook, optional): The source database hook.
        hook_sink (SQLServerHook, optional): The sink database hook.
        transform_callables (List[Callable], optional): List of transformation functions to apply.
        **kwargs: Additional keyword arguments for customization.

    Returns:
        int: The number of rows transformed.
    """
    print(f"[spark_transformation] starting transformation {refresh_type=}")
    transfer = SparkEngine(
        source_path=source_path,
        source_format=source_format,
        sink_path=sink_path,
        sink_format=sink_format,
    )

    default_func = lambda df: df.withColumns(dict(
        BATCH_JOB_ID=lit(kwargs.get("batch_job_id")),
        EDW_UPDATE_DTTM=lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ))
    if transform_callables:
        transform_callables += [default_func]
    else:
        transform_callables = [default_func]

    n_rows = transfer.transform(hook=hook_source,
                                hook_sink=hook_sink,
                                apply_transform_func=transform_callables,
                                refresh_type=refresh_type,
                                **kwargs)
    return n_rows


@ProvideBenchmark
@ProvideETLLogging
def spark_ingestion(source_path: str | list[str],
                    source_format: str,
                    sink_path: str,
                    sink_format: str,
                    file_name: str,

                    hook_sink: SQLServerHook,
                    transform_callables: List[Callable] = None,
                    **kwargs):
    """
    Handles the ingestion of data from a source to a sink using Spark.

    Args:
        source_path (str | list[str]): Path(s) to the source data.
        source_format (str): Format of the source data.
        sink_path (str): Path to the sink data.
        sink_format (str): Format of the sink data.
        file_name (str): Name of the file being ingested.
        hook_sink (SQLServerHook): The sink database hook.
        transform_callables (List[Callable], optional): List of transformation functions to apply. Default is None.
        **kwargs: Additional arguments for customization.

    Returns:
        int: The number of rows ingested.
    """
    # handle default column
    default_func = lambda df: df.withColumns(dict(
        BATCH_JOB_ID=lit(kwargs.get("batch_job_id")),
        EDW_UPDATE_DTTM=lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        FILE_NAME=lit(file_name)
    ))
    if transform_callables:
        transform_callables += [default_func]
    else:
        transform_callables = [default_func]

    # file_name below to be used in ingestion type cdc
    # add file_name to kwargs:
    kwargs["file_name"] = file_name

    # start main process
    transfer = SparkEngine(
        source_path=source_path,
        source_format=source_format,
        sink_path=sink_path,
        sink_format=sink_format,
    )

    n_rows = transfer.transform(hook=kwargs.get("hook_source"),
                                hook_sink=hook_sink,
                                apply_transform_func=transform_callables,
                                **kwargs)
    return n_rows


@ProvideBenchmark
def spark_ingestion_sftp(parent_directory: str,
                         file_name_pattern: str,
                         sink_path: str,

                         ingest_type: str | IngestType,
                         hook_sink: SQLServerHook = SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
                         sink_format: str = "mssql",
                         batch_size: int = cfg.Ingestion.SFTP_MAX_FILE_PER_RUN,
                         sftp_conn_id: str = "sftp_default",
                         transform_callables: List[Callable] = None,
                         **kwargs):
    """
    Ingests data files from an SFTP server into a sink using Spark.

    Args:
        parent_directory (str): Parent directory on the SFTP server.
        file_name_pattern (str): Pattern to match file names.
        sink_path (str): Path to the sink data.
        ingest_type (str | IngestType): Type of ingestion to perform.
        hook_sink (SQLServerHook): The sink database hook. Default is configured for EDW.
        sink_format (str): Format of the sink data. Default is "mssql".
        batch_size (int): Maximum number of files to process per run. Default is configured.
        sftp_conn_id (str): Connection ID for the SFTP hook. Default is "sftp_default".
        transform_callables (List[Callable], optional): List of transformation functions to apply. Default is None.
        **kwargs: Additional arguments for customization.

    Returns:
        dict: Details about ingested and processed files.
    """
    print("[spark_ingestion_sftp] start load list new files from sftp connection")
    sftp_hook = SFTPHook(sftp_conn_id)
    sync = SFTPFileSynchronize()

    force_rerun = kwargs.get("params", {}).get("force_rerun", False)
    df_new_files = sync.list_new_files(parent_directory=parent_directory,
                                       file_name_pattern=file_name_pattern,
                                       sftp_hook=sftp_hook,
                                       hook=hook_sink,
                                       sink_path=sink_path,
                                       ingest_type=ingest_type,
                                       force_rerun=force_rerun,
                                       )
    print_table(df_new_files)

    data = df_new_files["full_path"].tolist()
    data.sort()
    if ingest_type == IngestType.mtd:
        data = data[-1:]
    else:
        data = data[:batch_size + 1]  # reduce list to batch size

    # if having new files
    file_ingested = []
    if data:
        # process each file
        for m in data:
            with sftp_hook.download_with_contextmanager(m) as output:
                temp_file, flag_backup = output

                print(f"[spark_ingestion_sftp] start process file: {m}")
                file_name = Path(m).name

                print("[spark_ingestion_sftp] run ingestion in spark")
                spark_ingestion(source_path=str(temp_file),
                                source_format=temp_file.suffix.lower().removeprefix("."),
                                sink_path=sink_path,
                                sink_format=sink_format,
                                file_name=file_name,
                                hook_sink=hook_sink,
                                transform_callables=transform_callables,
                                ingest_type=ingest_type,
                                **kwargs
                                )

                print("[spark_ingestion_sftp] save log to db")
                data_dict = df_new_files[df_new_files["full_path"].eq(m)].to_dict('records')[0]
                data_dict.pop("full_path")
                SFTPFileSynchronize.save_record(**data_dict)

                file_ingested += [m]

                if flag_backup:
                    print("[spark_ingestion_sftp] backup raw file")
                    SFTPFileSynchronize.backup_raw_file(temp_file)

        return dict(file_ingested=file_ingested,
                    list_files=data, )


@ProvideBenchmark
def spark_ingestion_onedrive(file_name: str,
                             sink_path: str,

                             ingest_type: IngestType,
                             hook_sink: SQLServerHook = SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
                             sink_format: str = "mssql",
                             transform_callables: List[Callable] = None,
                             partition_column: str = None,
                             partition_value: str = None,
                             is_logging: bool = True,
                             **kwargs):
    """
    Ingests data from OneDrive to a sink using Spark.

    Args:
        file_name (str): Name of the file to ingest.
        sink_path (str): Path to the sink data.
        ingest_type (IngestType): Type of ingestion to perform.
        hook_sink (SQLServerHook): The sink database hook. Default is configured for EDW.
        sink_format (str): Format of the sink data. Default is "mssql".
        transform_callables (List[Callable], optional): List of transformation functions to apply. Default is None.
        partition_column (str, optional): Column used for partitioned ingestion. Default is None.
        partition_value (str, optional): Value to filter partitioned data. Default is None.
        is_logging (bool): Whether to log the operation. Default is True.
        **kwargs: Additional arguments for customization.

    Returns:
        None
    """
    # check file new version:
    is_need_update, file_info_dict = OneDriveFileSynchronize.check_file_need_update(file_name=file_name)

    force_rerun = kwargs.get("params", {}).get("force_rerun", False)

    if is_need_update or force_rerun:
        print("[spark_ingestion_sftp] run ingestion in spark")
        file_ext = Path(file_name).suffix.lower().removeprefix(".")

        spark_ingestion(source_path=f"{join(cfg.Ingestion.ONEDRIVE_LOCATION, file_name)}",
                        source_format=file_ext,
                        sink_path=sink_path,
                        sink_format=sink_format,
                        file_name=file_name,
                        hook_sink=hook_sink,
                        transform_callables=transform_callables,
                        partition_column=partition_column,
                        partition_value=partition_value,
                        ingest_type=ingest_type,
                        **kwargs
                        )

        if is_logging:
            print("[spark_ingestion_sftp] save log to db")
            onedrive_file_logging(**file_info_dict)


def onedrive_sync_file(source_filename_pattern: str,
                       source_base_folder: str,
                       sink_filename_pattern: str,
                       sink_base_folder: str = cfg.Ingestion.ONEDRIVE_LOCATION,
                       **kwargs
                       ):
    """
    Synchronizes files from a source to a OneDrive location.

    Args:
        source_filename_pattern (str): Pattern to match source filenames.
        source_base_folder (str): Base folder containing source files.
        sink_filename_pattern (str): Pattern for naming sink files.
        sink_base_folder (str): Base folder for sink files. Default is OneDrive location.
        **kwargs: Additional arguments, including `run_date` and `force_update`.

    Raises:
        AirflowSkipException: If no new file is found for synchronization.
    """
    action = OneDriveFileSynchronize.check_and_copy(
        source_filename_pattern=source_filename_pattern,
        source_base_folder=source_base_folder,
        sink_filename_pattern=sink_filename_pattern,
        sink_base_folder=sink_base_folder,
        run_date=kwargs.get('run_date'),
        force_update=kwargs.get('force_update'),
    )
    if not action:
        from airflow.exceptions import AirflowSkipException
        raise AirflowSkipException("Skip Job: No New File")


def onedrive_file_logging(**kwargs):
    OneDriveFileSynchronize.save_record(**kwargs)


@ProvideBenchmark
@ProvideETLLogging
def spark_upload_sftp(source_path: str,
                      sink_path: str,

                      hook_source: SQLServerHook = SQLServerHook(cfg.Hooks.MSSQL.production, database=cfg.Core.DB_NAME),
                      source_format: str = "mssql",
                      sink_format: str = "csv",
                      sftp_conn_id: str = "sftp_default",
                      **kwargs):
    """
    Uploads data from a source to an SFTP server using Spark.

    Args:
        source_path (str): Path to the source data.
        sink_path (str): Path on the SFTP server for the data.
        hook_source (SQLServerHook): The source database hook.
        source_format (str): Format of the source data. Default is "mssql".
        sink_format (str): Format of the sink data. Default is "csv".
        sftp_conn_id (str): Connection ID for the SFTP hook. Default is "sftp_default".
        **kwargs: Additional arguments for customization.

    Returns:
        int: The number of rows uploaded.
    """
    print("[spark_upload_sftp] start")
    sftp_hook = SFTPHook(sftp_conn_id)

    # get data
    transfer = SparkEngine(
        source_path=source_path,
        source_format=source_format,
        sink_path=sink_path,
        sink_format=sink_format,
    )

    tmp_path = "/tmp/" + Path(sink_path).stem + Path(sink_path).suffix
    print(f"[spark_upload_sftp] write data to tmp location: {tmp_path=}")
    transfer.sink_path = tmp_path
    n_rows = transfer.transform(hook=hook_source, write_mode="append", **kwargs)

    print(f"[spark_upload_sftp] copy file to sftp location {sink_path=}")
    with sftp_hook as client:
        client.put(localpath=tmp_path, remotepath=sink_path, callback=progress_callback)

    print("[spark_upload_sftp] remove temp file")
    os.unlink(tmp_path)

    return n_rows


@ProvideBenchmark
@ProvideETLLogging
def _spark_ingestion_batch(source_path: str | list[str],
                           source_format: str,
                           sink_path: str,
                           sink_format: str,

                           hook_sink: SQLServerHook,
                           transform_callables: List[Callable] = None,
                           **kwargs):
    # handle default column
    default_func = lambda df: df.withColumns(dict(
        BATCH_JOB_ID=lit(kwargs.get("batch_job_id")),
        EDW_UPDATE_DTTM=lit(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
        FILE_NAME=element_at(split(input_file_name(), '/'), -1),
    ))
    if transform_callables:
        transform_callables += [default_func]
    else:
        transform_callables = [default_func]

    # start main process
    transfer = SparkEngine(
        source_path=source_path,
        source_format=source_format,
        sink_path=sink_path,
        sink_format=sink_format,
    )

    n_rows = transfer.transform(hook=kwargs.get("hook_source"),
                                hook_sink=hook_sink,
                                apply_transform_func=transform_callables,
                                **kwargs)

    return n_rows


@ProvideBenchmark  # noqa
def spark_ingestion_batch_onedrive(file_name_pattern: str,
                                   data_address: list[str],
                                   sink_path: str,

                                   sink_format: str = "mssql",

                                   ingest_type: IngestType = IngestType.cdc,
                                   hook_sink: SQLServerHook = SQLServerHook(cfg.Hooks.MSSQL.new, database=cfg.Core.DB_NAME),
                                   transform_callables: List[Callable] = None,
                                   is_logging: bool = True,

                                   **kwargs):
    """
    Handles batch ingestion of multiple files from OneDrive into a sink using Spark.

    Args:
        file_name_pattern (str): Pattern to match file names.
        data_address (list[str]): List of data addresses.
        sink_path (str): Path to the sink data.
        sink_format (str): Format of the sink data. Default is "mssql".
        ingest_type (IngestType): Type of ingestion to perform. Default is CDC.
        hook_sink (SQLServerHook): The sink database hook.
        transform_callables (List[Callable], optional): List of transformation functions to apply. Default is None.
        is_logging (bool): Whether to log the operation. Default is True.
        **kwargs: Additional arguments for customization.

    Returns:
        None
    """
    force_rerun = kwargs.get("params", {}).get("force_rerun", False)
    file_names = glob(file_name_pattern)

    # check if any file is changed (compare hash)
    files_logging = []
    files_need_to_update = []
    print(f"[spark_ingestion_batch_onedrive] start check hash all files")
    for file_name in file_names:
        is_need_update, file_logging = OneDriveFileSynchronize.check_file_need_update(file_name=file_name)
        files_need_to_update += [is_need_update]
        files_logging += [file_logging]

    # if any then process all file at the same time
    # consider only update file changed if mode cdc
    print(f"[spark_ingestion_batch_onedrive] start ingest all file as a batch")
    is_success = False
    if any(files_need_to_update) or force_rerun:
        file_ext = Path(file_names[0]).suffix.lower().removeprefix(".")
        _spark_ingestion_batch(source_path=file_names,
                               source_format=file_ext,
                               sink_path=sink_path,
                               sink_format=sink_format,
                               hook_sink=hook_sink,
                               transform_callables=transform_callables,
                               ingest_type=ingest_type,
                               data_address=data_address,
                               **kwargs
                               )
        is_success = True

    # logging new info (include new hash)
    print(f"[spark_ingestion_batch_onedrive] {is_success=} {is_logging=}")
    if is_success and is_logging:
        for m in files_logging:
            print(f"[spark_ingestion_batch_onedrive] {m=}")
            onedrive_file_logging(**m)
