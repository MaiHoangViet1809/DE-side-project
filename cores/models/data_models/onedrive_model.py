from sqlalchemy import Column, Integer, DATETIME, VARCHAR, BigInteger  # DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import PrimaryKeyConstraint
from pandas import DataFrame, to_datetime

# from cores.hooks.mssql import SQLServerHook
from cores.utils.configs import FrameworkConfigs as cfg
# from cores.utils.providers import ProvideHookFramework
from cores.utils.checksum import check_sum
from cores.utils.shells import run_sh
from cores.utils.functions import human_readable_size
from cores.models.data_models.data_model_creation import BehaviorStandardDataModel
from cores.utils.debug import print_table

from os.path import join
from pathlib import Path

# declarative base class
_Base = declarative_base()

# base folder of onedrive
_BASE_FOLDER = cfg.Ingestion.ONEDRIVE_LOCATION

"""
manual fix:
select OBJECT_NAME(OBJECT_ID) AS NameofConstraint
FROM sys.objects
where OBJECT_NAME(parent_object_id)='TBL_ONEDRIVE_FILE_LOG'
and type_desc LIKE '%CONSTRAINT'
alter table CONFIG.TBL_ONEDRIVE_FILE_LOG drop CONSTRAINT PK__TBL_ONED__EE0A6EA5332A379F;
alter table CONFIG.TBL_ONEDRIVE_FILE_LOG add primary key (file_name, hash_check_sum);
"""


class FileHelper:
    """
    A utility class for handling file operations, including path generation,
    file metadata retrieval, checksum calculation, and file synchronization.

    Methods:
        build_file_path(cls, file_name: str, base_folder: str, check_exist: bool):
            Constructs the full path to a file and optionally checks if it exists.
        read_file_stat(cls, file_path: str):
            Reads file metadata such as size and last modification time.
        read_file_checksum(file_size: int, file_path: str):
            Calculates the checksum of a file.
        find_latest_file(filename_pattern: str, base_folder: str):
            Finds the latest file matching a pattern in a specified folder.
        sync_new_file(source_file: str, sink_file: str):
            Synchronizes a file from the source to the sink location.
    """

    @classmethod
    def build_file_path(cls, file_name: str, base_folder: str = _BASE_FOLDER, check_exist=True):
        """
        Constructs the full path to a file and optionally checks if it exists.

        Args:
            file_name (str): Name of the file.
            base_folder (str): Base folder for the file. Default is `_BASE_FOLDER`.
            check_exist (bool): Whether to check if the file exists. Default is True.

        Returns:
            str: Full file path if the file exists or `check_exist` is False.
            None if the file does not exist.
        """
        file_full_path = join(base_folder, file_name)
        if Path(file_full_path).exists() or not check_exist:
            return file_full_path
        else:
            print(f"[OneDriveFileSynchronize.build_file_path] {file_full_path=} not exist")

    @classmethod
    def read_file_stat(cls, file_path: str):
        """
         Reads file metadata such as size and last modification time.

         Args:
             file_path (str): Path to the file.

         Returns:
             tuple: A tuple containing file name, size (in bytes), and last modification time.
         """
        rtcode, stdout = run_sh(f"""stat -c "%n|%s|%Y" -t "{file_path}" """)
        print(f"[read_file_stat] {rtcode=} {stdout=}")
        file_name, st_size, st_mtime = str(stdout[0]).split("|")
        return file_name, int(st_size), st_mtime

    @staticmethod
    def read_file_checksum(file_size: int, file_path: str):
        """
        Calculates the checksum of a file.

        Args:
            file_size (int): Size of the file in bytes.
            file_path (str): Path to the file.

        Returns:
            str: The checksum of the file.
        """
        return check_sum(file_path=file_path, total_bytes=file_size)

    @staticmethod
    def find_latest_file(filename_pattern: str, base_folder: str = _BASE_FOLDER):
        """
        Finds the latest file matching a pattern in a specified folder.

        Args:
            filename_pattern (str): The pattern to match filenames.
            base_folder (str): The base folder to search in. Default is `_BASE_FOLDER`.

        Returns:
            str: The name of the latest file matching the pattern.
            None if no file matches the pattern.
        """
        from glob import glob
        list_files = glob(join(base_folder, filename_pattern))
        if list_files:
            list_files.sort()
            latest_file = list_files[-1]
            return Path(latest_file).parts[-1]

    @staticmethod
    def sync_new_file(source_file: str, sink_file: str):
        """
        Synchronizes a file from the source to the sink location.

        Args:
            source_file (str): Path to the source file.
            sink_file (str): Path to the sink file.
        """
        from shutil import copyfile
        dest_path = Path(_BASE_FOLDER) / Path(sink_file)
        copyfile(src=Path(source_file), dst=dest_path)
        print("[sync_new_file] done")


class OneDriveFileSynchronize(_Base, BehaviorStandardDataModel, FileHelper):
    """
    A class for managing file synchronization and metadata logging for OneDrive files.

    Inherits from:
        - `_Base`: The declarative base for SQLAlchemy ORM models.
        - `BehaviorStandardDataModel`: Provides standard data model behavior.
        - `FileHelper`: Utility methods for file operations.

    Attributes:
        file_name (Column): The name of the file.
        last_modified_dt (Column): The last modification timestamp of the file.
        file_size (Column): The size of the file as a human-readable string.
        st_size (Column): The size of the file in bytes.
        st_mtime (Column): The last modification time of the file as an integer.
        hash_check_sum (Column): The checksum of the file.
    """
    __tablename__ = "TBL_ONEDRIVE_FILE_LOG"
    __table_args__ = (
        PrimaryKeyConstraint("file_name", "hash_check_sum"),
        {"schema": cfg.Core.SCHEMA_EDW_FRAMEWORK},
    )
    file_name        = Column(VARCHAR(255))
    last_modified_dt = Column(DATETIME)
    file_size        = Column(VARCHAR(50))
    st_size          = Column(BigInteger)
    st_mtime         = Column(Integer)
    hash_check_sum   = Column(VARCHAR(1000))


    @classmethod
    def get_info_loaded_file(cls, file_name: str):
        """
        Retrieves metadata for the last loaded file matching the given name.

        Args:
            file_name (str): The name of the file.

        Returns:
            dict: Metadata for the last loaded file, or an empty dictionary if none exists.
        """
        df = cls.get_records(conditions=(OneDriveFileSynchronize.file_name == file_name))
        df = df.reset_index(drop=True).sort_values(["st_mtime"], ascending=[False])
        # print_table(df, 5)
        return df.to_dict('records')[-1] if len(df) > 0 else {}

    @classmethod
    def get_info_current_file(cls, file_name: str, base_folder: str = _BASE_FOLDER) -> dict:
        """
        Retrieves metadata for the current file in the specified folder.

        Args:
            file_name (str): The name of the file.
            base_folder (str): The folder containing the file. Default is `_BASE_FOLDER`.

        Returns:
            dict: Metadata for the current file.
        """
        file_path = cls.build_file_path(file_name=file_name, base_folder=base_folder)

        if file_path:
            full_file_name, st_size, st_mtime = cls.read_file_stat(file_path=file_path)
            hash_check_sum = cls.read_file_checksum(file_size=st_size, file_path=file_path)

            df = (
                DataFrame([{
                    "file_name": file_name,
                    "st_size": st_size,
                    "st_mtime": st_mtime,
                    "hash_check_sum": hash_check_sum
                }])
                .assign(
                    last_modified_dt=lambda x: to_datetime(x['st_mtime'], unit='s'),
                    file_size=lambda x: x['st_size'].apply(human_readable_size),
                )
            )

            df = df[cls.__table__.columns.keys()]
            return df.to_dict('records')[0]

    @classmethod
    def check_file_need_update(cls, file_name: str, base_folder: str = _BASE_FOLDER):
        """
        Checks if a file needs to be updated based on checksum comparison.

        Args:
            file_name (str): The name of the file.
            base_folder (str): The folder containing the file. Default is `_BASE_FOLDER`.

        Returns:
            tuple: A tuple containing a boolean indicating if an update is needed
            and metadata for the current file.
        """
        prev_file = cls.get_info_loaded_file(file_name=file_name)
        current_file = cls.get_info_current_file(file_name=file_name, base_folder=base_folder)
        print(f"{prev_file=}")
        print(f"{current_file=}")
        if current_file:
            return prev_file.get("hash_check_sum") != current_file["hash_check_sum"], current_file
        else:
            return False, ""

    @classmethod
    def check_and_copy(cls, source_filename_pattern: str, source_base_folder: str, sink_filename_pattern: str, sink_base_folder: str, run_date: str, **kwargs):
        """
        Checks if a source file differs from the latest sink file and copies it if necessary.

        Args:
            source_filename_pattern (str): The filename pattern for source files.
            source_base_folder (str): The folder containing source files.
            sink_filename_pattern (str): The filename pattern for sink files.
            sink_base_folder (str): The folder containing sink files.
            run_date (str): The run date in 'YYYY-MM-DD' format.
            **kwargs: Additional arguments, such as `force_update` to override checks.

        Returns:
            bool: True if the file was updated, False otherwise.
        """
        latest_source_file = cls.find_latest_file(filename_pattern=source_filename_pattern, base_folder=source_base_folder)
        if not latest_source_file:
            print(f"File {source_filename_pattern=} is not found in path {source_base_folder=}")
            return False

        latest_source_file_info = cls.get_info_current_file(file_name=latest_source_file, base_folder=source_base_folder)
        latest_source_file_hash = latest_source_file_info.get("hash_check_sum")

        latest_sink_file = cls.find_latest_file(filename_pattern=sink_filename_pattern, base_folder=sink_base_folder)
        latest_sink_file_hash = None
        if latest_sink_file:
            latest_sink_file_info = cls.get_info_current_file(file_name=latest_sink_file, base_folder=sink_base_folder)
            latest_sink_file_hash = latest_sink_file_info.get("hash_check_sum")

        print(f"{latest_source_file=} {latest_source_file_hash=}")
        print(f"{latest_sink_file=} {latest_sink_file_hash=}")

        new_file_name = sink_filename_pattern
        if "*" in sink_filename_pattern:
            # add new timestamp to file name <original pattern>_YYYYMMDD.old_suffix
            new_file_suffix = run_date.replace("-", "")
            new_file_name = Path(sink_filename_pattern).stem.removesuffix("*").removesuffix("_") + "_" + new_file_suffix + Path(sink_filename_pattern).suffix

        is_force_update = kwargs.get("force_update")
        if latest_source_file_hash != latest_sink_file_hash or is_force_update:
            cls.sync_new_file(source_file=cls.build_file_path(file_name=latest_source_file, base_folder=source_base_folder),
                              sink_file=cls.build_file_path(file_name=new_file_name, base_folder=sink_base_folder, check_exist=False))
        else:
            print(f"[check_and_copy] DO NOTHING - no new file - {latest_source_file_hash != latest_sink_file_hash=}")

        return latest_source_file_hash != latest_sink_file_hash


if __name__ == "__main__":
    OneDriveFileSynchronize.create_log_table()

