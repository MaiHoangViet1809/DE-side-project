
from cores.hooks.mssql import SQLServerHook
from pandas import DataFrame


class MSSQLMaintainer:
    """
    A class for managing and maintaining Microsoft SQL Server operations using the SQLServerHook.

    Attributes:
        hook (SQLServerHook): An instance of the SQLServerHook to interact with the SQL Server database.
    """
    def __init__(self, hook: SQLServerHook):
        """
        Initializes the MSSQLMaintainer class.

        Args:
            hook (SQLServerHook): The SQLServerHook instance used for database operations.
        """
        self.hook = hook

    def get_list_cci(self) -> DataFrame:
        """
        Retrieves a list of clustered columnstore indexes (CCI) from the SQL Server database.

        Returns:
            DataFrame: A Pandas DataFrame containing:
                - FULL_NAME: Full name of the table (schema and table name).
                - CCI_NAME: Name of the clustered columnstore index.
                - COMMAND: SQL command to reorganize the clustered columnstore index.
        """
        df = self.hook.get_pandas_df(sql="""
        SELECT CONCAT(SCHEMA_NAME, ',', TABLE_NAME) AS FULL_NAME,
               CCI_NAME,
               CONCAT('ALTER INDEX ', CCI_NAME, ' ON ', SCHEMA_NAME, '.', TABLE_NAME, ' REORGANIZE') COMMAND
          FROM CONFIG.VW_HELPER_TABLES_SIZE
         WHERE CCI_NAME IS NOT NULL
        """)
        return df

    def run_command(self, command: str):
        """
        Executes a SQL command on the SQL Server database.

        Args:
            command (str): The SQL command to execute.
        """
        self.hook.run_sql(sql=command)
