from pyodbc import Connection
from contextlib import closing

from cores.hooks.mssql import SQLServerHook
from cores.utils.configs import FrameworkConfigs as cfg


from cores.models.data_models.event_model import TableEvent
from cores.models.data_models.sftp_model import SFTPFileSynchronize
from cores.models.data_models.onedrive_model import OneDriveFileSynchronize


def create_database(db_name: str, conn: Connection, default_path: str = "H:\\EDW-PROJECT"):
    """
    Creates a database if it does not already exist.

    Args:
        db_name (str): The name of the database to create.
        conn (Connection): A pyodbc database connection.
        default_path (str): The default file path for the database files. Default is "H:\\EDW-PROJECT".

    Process:
        - Checks if the database exists.
        - If not, creates the database with specified file paths for data and log files.
    """
    db_name = db_name.upper()
    sql = f"""
    IF NOT EXISTS (SELECT * FROM sys.databases WHERE NAME = N'{db_name}' )
    CREATE DATABASE {db_name}
    ON
    ( NAME = {db_name}_DATA,
        FILENAME = '{default_path}\\{db_name}_DATA.mdf',
        SIZE = 16MB,
        MAXSIZE = 128MB,
        FILEGROWTH = 16MB )
    LOG ON
    ( NAME = {db_name}_LOG,
        FILENAME = '{default_path}\\{db_name}_LOG.ldf',
        SIZE = 8MB,
        MAXSIZE = 64MB,
        FILEGROWTH = 8MB )
    """
    conn.autocommit = True
    conn.execute(sql)
    conn.autocommit = False


def create_schema_if_not_exist(schema_name: str, conn: Connection, db_name="EDW"):
    """
    Creates a schema in the specified database if it does not already exist.

    Args:
        schema_name (str): The name of the schema to create.
        conn (Connection): A pyodbc database connection.
        db_name (str): The name of the database. Default is "EDW".

    Process:
        - Checks if the schema exists in the specified database.
        - If not, creates the schema with `dbo` as the owner.
    """
    sql = f"""
    IF NOT EXISTS (SELECT 1 FROM {db_name}.sys.schemas WHERE UPPER(name) = UPPER('{schema_name}') )
    BEGIN
        USE {db_name}
        EXEC('CREATE SCHEMA {schema_name} AUTHORIZATION [dbo]')
    END
    """
    conn.autocommit = True
    try:
        conn.execute(sql)
    except:
        print("ERROR - SQL:\n", sql)
    conn.autocommit = False


if __name__ == "__main__":
    """
    Main execution block to create databases, schemas, and log tables.

    Process:
        - Connects to the SQL Server using production and UAT hooks.
        - Creates databases and schemas in production and UAT environments.
        - Creates log tables in the production database.
    """
    # this file will be executed in the dask docker before dask cluster start in docker deployment
    # or execute in normal environment in WSL1 Python deployment
    print("create table framework for PROD")
    hook_master = SQLServerHook(cfg.Hooks.MSSQL.production, database="master")

    with closing(hook_master.get_conn()) as conn:
        print(f"start create database {cfg.Core.DB_NAME} in production")
        create_database(cfg.Core.DB_NAME, conn=conn, default_path="H:\\EDW-PROJECT")  # path in VM production
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_FRAMEWORK, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_INGEST, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_STAGING, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_CONSOLIDATE, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_DATAMART, conn=conn, db_name=cfg.Core.DB_NAME)

    hook_master_ecom = SQLServerHook(cfg.Hooks.MSSQL.new, database="master")
    with closing(hook_master_ecom.get_conn()) as conn:
        print(f"start create database {cfg.Core.DB_NAME} in uat ecom")
        create_database(cfg.Core.DB_NAME, conn=conn, default_path="C:\\DatabaseFile\\EDW-PROJECT")  # path in VM ecom
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_FRAMEWORK, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_INGEST, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_STAGING, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_CONSOLIDATE, conn=conn, db_name=cfg.Core.DB_NAME)
        create_schema_if_not_exist(schema_name=cfg.Core.SCHEMA_EDW_DATAMART, conn=conn, db_name=cfg.Core.DB_NAME)

    print("start create log table in production db")
    TableEvent.create_log_table()
    SFTPFileSynchronize.create_log_table()
    OneDriveFileSynchronize.create_log_table()
