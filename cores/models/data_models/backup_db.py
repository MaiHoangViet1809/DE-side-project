from sqlalchemy import Column, DATETIME, VARCHAR  # DATE
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import PrimaryKeyConstraint

from cores.utils.configs import FrameworkConfigs as cfg
from cores.hooks.mssql import SQLServerHook

from cores.models.data_models.data_model_creation import BehaviorStandardDataModel

# declarative base class
_Base = declarative_base()


class BackupDB(_Base, BehaviorStandardDataModel):
    """
    A SQLAlchemy ORM class representing the backup database log table.

    Inherits from:
        - `_Base`: The declarative base for SQLAlchemy ORM models.
        - `BehaviorStandardDataModel`: Provides standard data model behavior.

    Attributes:
        __tablename__ (str): The name of the table.
        __table_args__ (tuple): Table configuration, including schema and primary key constraints.
        DATABASE_NAME (Column): The name of the database (VARCHAR(255)).
        BACKUP_FILE (Column): The backup file name (VARCHAR(255)).
        BACKUP_DATETIME (Column): The datetime of the backup (DATETIME).
    """
    __tablename__ = "TBL_BACKUP_DB_LOG"
    __table_args__ = (
        PrimaryKeyConstraint("DATABASE_NAME", "BACKUP_FILE"),
        {"schema": cfg.Core.SCHEMA_EDW_FRAMEWORK},
    )

    DATABASE_NAME    = Column(VARCHAR(255))
    BACKUP_FILE      = Column(VARCHAR(255))
    BACKUP_DATETIME  = Column(DATETIME)

    @classmethod
    def execute_backup(cls, hook: SQLServerHook):
        """
        Executes a full backup of the database and logs the operation.

        Args:
            hook (SQLServerHook): A database connection hook to execute the backup command.

        Raises:
            Exception: If the backup operation fails, the exception is raised with a traceback.

        Process:
            - Generates a backup file name based on the current timestamp.
            - Executes the SQL Server `BACKUP DATABASE` command.
            - Logs the backup operation in the `TBL_BACKUP_DB_LOG` table.
        """
        from datetime import datetime

        db_name = cfg.Core.DB_NAME
        backup_dir = cfg.Core.DB_BACKUP_DIRECTORY
        backup_datetime = datetime.now()
        backup_file = f"edw_backup_{backup_datetime.strftime('%Y%m%d%H%M%S')}.bak"

        backup_command = rf"""
        BACKUP DATABASE {db_name} TO DISK = '{backup_dir}\{backup_file}' 
        WITH FORMAT, 
             COMPRESSION, 
             MEDIANAME = 'SQLServerBackups', 
             NAME = 'Full Backup of {db_name}'
        """
        conn = hook.get_conn()

        try:
            # Backup command
            conn.autocommit = True
            with conn.cursor() as cursor:
                cursor = cursor.execute(backup_command)
                while cursor.nextset():
                    pass

            # write log
            cls.save_record(DATABASE_NAME=db_name, BACKUP_FILE=backup_file, BACKUP_DATETIME=backup_datetime)
        except Exception as e:
            import traceback
            traceback.print_exc()
            conn.close()
            raise e

    @classmethod
    def cleanup_old_backup(cls, n_backups_retention: int = 2):
        """
        Cleans up old backup files, retaining only a specified number of recent backups.

        Args:
            n_backups_retention (int): The number of recent backups to retain. Default is 2.

        Process:
            - Lists all backup files matching the pattern `edw_backup_*.bak`.
            - Sorts the files in reverse chronological order.
            - Deletes files exceeding the retention limit.
        """
        from glob import glob
        from os.path import join
        from pathlib import Path
        list_file_backups = glob(join(cfg.Core.DB_BACKUP_DIRECTORY.replace("E:\\", "/mnt/e/"), "edw_backup_*.bak"))
        list_file_backups.sort(reverse=True)
        print(f"[cleanup_old_backup] {list_file_backups=}")

        list_delete_files = list_file_backups[n_backups_retention:]
        print(f"[cleanup_old_backup] {list_delete_files=}")

        for m in list_delete_files:
            Path(m).unlink()
            print(f"[cleanup_old_backup] successfully remove {m=}")


if __name__ == "__main__":
    BackupDB.create_log_table()
