"""Microsoft SQLServer hook module."""
import asyncio
from typing import Any, TYPE_CHECKING
from pyodbc import Connection

from airflow.providers.common.sql.hooks.sql import DbApiHook as _DbApiHook

__all__ = [
    "SQLServerHook"
]


class _MsSqlHook(_DbApiHook):
    """
    Interact with Microsoft SQL Server.

    Attributes:
        conn_name_attr (str): Connection attribute name.
        default_conn_name (str): Default connection name.
        conn_type (str): Type of connection.
        hook_name (str): Display name of the hook.
        supports_autocommit (bool): Indicates support for autocommit.
        DEFAULT_SQLALCHEMY_SCHEME (str): Default SQLAlchemy scheme.

    Methods:
        connection_extra_lower: Retrieves connection extras in lowercase keys.
        sqlalchemy_scheme: Retrieves the SQLAlchemy scheme.
        get_uri: Constructs the connection URI.
        get_sqlalchemy_connection: Retrieves a SQLAlchemy connection.
        get_conn: Retrieves a pyodbc connection.
        get_connection_uri: Constructs a connection URI with an ODBC string.
    """

    conn_name_attr = "mssql_conn_id"
    default_conn_name = "mssql_default"
    conn_type = "mssql"
    hook_name = "Microsoft SQL Server"
    supports_autocommit = True
    DEFAULT_SQLALCHEMY_SCHEME = "mssql+pyodbc"

    def __init__(
        self,
        *args,
        sqlalchemy_scheme: str | None = None,
        **kwargs,
    ) -> None:
        """
        Initializes the _MsSqlHook.

        Args:
            *args: Positional arguments for the base class.
            sqlalchemy_scheme (str | None): SQLAlchemy scheme.
            **kwargs: Additional keyword arguments.
        """
        from cores.utils.configs import FrameworkConfigs
        super().__init__(*args, **kwargs)
        self.database = kwargs.pop("database", FrameworkConfigs.Core.DB_NAME)
        self.schema = kwargs.pop("schema", FrameworkConfigs.Core.SCHEMA_EDW_DEFAULT)
        self._sqlalchemy_scheme = sqlalchemy_scheme
        self._SessionCLS = None
        self._session = None

    @property
    def connection_extra_lower(self) -> dict:
        """
        Retrieves connection extras in lowercase keys.

        Returns:
            dict: Connection extras with lowercase keys.
        """
        conn = self.get_connection(self.mssql_conn_id)  # type: ignore[attr-defined]
        return {k.lower(): v for k, v in conn.extra_dejson.items()}

    @property
    def sqlalchemy_scheme(self) -> str:
        """
        Retrieves the SQLAlchemy scheme.
        Sqlalchemy scheme either from constructor, connection extras or default.

        Returns:
            str: SQLAlchemy scheme.
        """
        extra_scheme = self.connection_extra_lower.get("sqlalchemy_scheme")
        if not self._sqlalchemy_scheme and extra_scheme and (":" in extra_scheme or "/" in extra_scheme):
            raise RuntimeError("sqlalchemy_scheme in connection extra should not contain : or / characters")
        return self._sqlalchemy_scheme or extra_scheme or self.DEFAULT_SQLALCHEMY_SCHEME

    def get_uri(self) -> str:
        """
        Constructs the connection URI.

        Returns:
            str: Connection URI.
        """
        from urllib.parse import parse_qs, urlencode, urlsplit, urlunsplit

        r = list(urlsplit(super().get_uri()))
        # change pymssql driver:
        r[0] = self.sqlalchemy_scheme
        # remove query string 'sqlalchemy_scheme' like parameters:
        qs = parse_qs(r[3], keep_blank_values=True)
        for k in list(qs.keys()):
            if k.lower() == "sqlalchemy_scheme":
                qs.pop(k, None)
        r[3] = urlencode(qs, doseq=True)
        return urlunsplit(r)

    def get_sqlalchemy_connection(self, connect_kwargs: dict | None = None, engine_kwargs: dict | None = None) -> Any:
        """
        Retrieves a SQLAlchemy connection object.

        Args:
            connect_kwargs (dict | None): Additional connection arguments.
            engine_kwargs (dict | None): Additional engine arguments.

        Returns:
            Any: SQLAlchemy connection object.
        """
        engine = self.get_sqlalchemy_engine(engine_kwargs=engine_kwargs)
        return engine.connect(**(connect_kwargs or {}))

    def get_conn(self) -> Connection:
        """
        Retrieves a pyodbc connection.

        Returns:
            Connection: Pyodbc connection instance.
        """
        from pyodbc import connect
        conn = self.get_connection(self.mssql_conn_id)  # type: ignore[attr-defined]
        conn = connect(
            server=conn.host,
            user=conn.login,
            password=conn.password,
            database=self.database or conn.schema,
            port=conn.port,
            driver="{ODBC Driver 17 for SQL Server}",
            timeout=300,
            Encrypt="yes",  # keep traffic encrypted
            TrustServerCertificate="yes",  # skip chain validation
        )
        return conn

    def get_connection_uri(self):
        """
        Constructs a connection URI with an ODBC string.

        Returns:
            str: Connection URI.
        """


        conn = self.get_connection(self.mssql_conn_id)  # type: ignore[attr-defined]
        # odbc_str = (
        #     r"Driver=ODBC Driver 17 for SQL Server;"
        #     fr"Server={conn.host};"  # or  tcp:{conn.host},{conn.port}
        #     fr"Database={self.database or conn.schema};"
        #     fr"UID={conn.login};"
        #     fr"PWD={conn.password};"
        #     "Encrypt=yes;"  # keep traffic encrypted
        #     "TrustServerCertificate=yes;"  # skip cert-chain validation
        #     "LoginTimeout=30;"
        # )
        # # odbc_str = fr"Driver=ODBC Driver 17 for SQL Server;Server={conn.host};Database={self.database or conn.schema};UID={conn.login};PWD={conn.password};"
        # connection_uri = f"mssql+pyodbc:///?odbc_connect={odbc_str}"

        from sqlalchemy.engine import URL
        url = URL.create(
            "mssql+pyodbc",
            username=conn.login,
            password=conn.password,
            host=conn.host,
            port=conn.port,  # optional; fold into host if you like
            database=self.database or conn.schema,
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "Encrypt": "yes",
                "TrustServerCertificate": "yes",
                "LoginTimeout": "30",
            },
        )
        connection_uri = str(url)
        return connection_uri


class SQLServerHook(_MsSqlHook):
    """
    Extended SQLServer Hook for interacting with SQL Server databases.

    Methods:
        get_table_scheme: Retrieves the schema of a table.
        run_sql: Executes an SQL query with optional logging.
        is_table_exists: Checks if a table exists in the database.
        insert_many: Inserts multiple rows into a destination table.
        create_engine: Creates a SQLAlchemy engine.
        create_session: Creates a SQLAlchemy session.
        close_session: Closes the SQLAlchemy session.
        query_to_list: Converts query results to a list.
        get_connection: Retrieves the database connection object.
    """
    from pandas import DataFrame

    def get_table_scheme(self, table_name: str, schema: str = None) -> DataFrame:
        """
        Retrieves the schema of a table.

        Args:
            table_name (str): Name of the table.
            schema (str, optional): Schema of the table. Defaults to None.

        Returns:
            DataFrame: Schema of the table.
        """
        schema = schema or self.schema
        if "." in table_name:
            print("[get_table_scheme] split schema and table from table name")
            schema, table_name = table_name.split(".")

        sql = f"""
            SELECT COL.COLUMN_ID,
                   COL.NAME AS COL_NAME,
                   T.NAME AS DATA_TYPE,
                   COL.MAX_LENGTH,
                   COL.PRECISION,
                   COL.IS_NULLABLE
              FROM SYS.TABLES AS TAB
                   INNER JOIN SYS.COLUMNS AS COL ON TAB.OBJECT_ID = COL.OBJECT_ID
                    LEFT JOIN SYS.TYPES AS T ON COL.USER_TYPE_ID = T.USER_TYPE_ID
             WHERE TAB.NAME = UPPER('{table_name}')
               AND SCHEMA_NAME(TAB.SCHEMA_ID) = UPPER('{schema}')
            ORDER BY TAB.NAME, COLUMN_ID
        """
        DF_SCHEMA = self.get_pandas_df(sql, index_col="COL_NAME")
        return DF_SCHEMA

    def run_sql(self, sql: str, log_sql = False):
        """
        Executes an SQL query with optional logging.

        Args:
            sql (str): SQL query string.
            log_sql (bool, optional): Whether to log the SQL query. Defaults to False.

        Returns:
            Any: Query execution result.
        """
        self.log_sql = log_sql
        return self.run(sql=sql, autocommit=True)

    def is_table_exists(self, table_name: str, schema_name: str):
        """
        Checks if a table exists in the database.

        Args:
            table_name (str): Name of the table.
            schema_name (str): Schema of the table.

        Returns:
            bool: True if the table exists, False otherwise.
        """
        df = self.get_pandas_df(f"""
                SELECT * 
                  FROM INFORMATION_SCHEMA.TABLES 
                 WHERE TABLE_SCHEMA = '{schema_name}' 
                   AND TABLE_NAME    = '{table_name}'
                """)
        return df.size > 0

    def insert_many(self, dst_table: str, data: list):
        """
        Inserts multiple rows into a destination table.

        Args:
            dst_table (str): Destination table name.
            data (list): List of data rows to insert.
        """
        from pyodbc import Cursor
        from contextlib import closing
        from random import randint
        params = ",".join(["?"] * len(data[0]))

        if "." in dst_table:
            schema, dst_table = dst_table.split(".")
        else:
            raise ValueError(f"dst_table must have schema and table - current={dst_table}")

        print(f"start import DB={self.database} schema={self.schema}")
        with closing(self.get_cursor()) as cursor:
            cursor: Cursor
            cursor.fast_executemany = True

            temp_table = f"##{dst_table}_{randint(100000, 999999)}"

            print("start create temp table:", temp_table, " from:", f"{schema}.{dst_table}")
            sql = f"SELECT * INTO {temp_table} FROM {self.database}.{schema}.{dst_table} WHERE 1=0"
            cursor.execute(sql)

            print("start insert data into temp table")
            stmt = f"INSERT INTO {temp_table} VALUES ({params})"
            try:
                cursor.executemany(stmt, data)
            except Exception as e:
                print(f"Error: {stmt=} {data[0]=}")
                print("test manual:", f"INSERT INTO {schema}.{dst_table} VALUES ({data[0]})")
                raise e

            print("start insert data into original table")
            cursor.execute(f"INSERT INTO {self.database}.{schema}.{dst_table} SELECT * FROM {temp_table}")
            cursor.commit()

            print("drop temp table")
            cursor.execute(f"DROP TABLE IF EXISTS {temp_table}")

    def create_engine(self):
        """
        Creates a SQLAlchemy engine.

        Returns:
            Engine: SQLAlchemy engine.
        """
        from sqlalchemy import create_engine
        return create_engine(url=self.get_connection_uri(), future=True)

    def create_session(self):
        """
        Creates a SQLAlchemy session.

        Returns:
            Session: SQLAlchemy session.
        """
        if not self._SessionCLS:
            from sqlalchemy.orm import sessionmaker
            self._SessionCLS = sessionmaker(bind=self.create_engine())

        if not self._session:
            self._session = self._SessionCLS()

        return self._session

    def close_session(self):
        """
        Closes the SQLAlchemy session.

        Returns:
            None
        """
        if self._session:
            self._session.close()
            self._session = None

    def __enter__(self):
        return self.create_session()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close_session()

    async def __aenter__(self):
        return await asyncio.to_thread(self.create_session)

    async def __aexit__(self, exc_type, exc, tb):
        await asyncio.to_thread(self.close_session)

    @staticmethod
    def query_to_list(list_of_rows: list):
        """
        Converts query results to a list.

        Args:
            list_of_rows (list): List of rows from a query.

        Returns:
            tuple: Keys and list of row values.
        """
        from sqlalchemy.inspection import inspect
        result = []
        keys = []
        for obj in list_of_rows:
            instance = inspect(obj)
            if not keys:
                keys = instance.attrs.keys()
            result.append([x.value for _, x in instance.attrs.items()])
        return keys, result

    # @classmethod
    def get_connection(self, conn_id: str) -> Connection:
        """
        Retrieves the database connection object.

        Args:
            conn_id (str): Connection ID.

        Returns:
            Connection: Database connection object.
        """
        from airflow.models.connection import Connection
        conn = Connection.get_connection_from_secrets(conn_id)
        if self.log_sql:
            print(f"Using connection ID {conn.conn_id=} - {conn.host} - {conn.schema=} for task execution.")
        return conn

