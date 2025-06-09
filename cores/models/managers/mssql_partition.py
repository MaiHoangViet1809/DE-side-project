from datetime import datetime
from typing import Literal
from dateutil.relativedelta import relativedelta

from cores.utils.configs import FrameworkConfigs as cfg

"""
official documentation:
https://learn.microsoft.com/en-us/sql/t-sql/statements/alter-table-transact-sql?view=sql-server-2017

limitation 15000 partitions;

CREATE PARTITION FUNCTION salesYearPartitions (datetime)
AS RANGE RIGHT FOR VALUES (
'2009-01-01',
'2010-01-01'
)

CREATE PARTITION SCHEME Test_PartitionScheme
AS PARTITION salesYearPartitions
ALL TO ('PRIMARY')

CREATE TABLE SalesArchival (
    SaleTime datetime PRIMARY KEY,
    ItemName varchar(50)
)
ON Test_PartitionScheme (SaleTime);

SELECT * FROM sys.partition_functions;
SELECT * FROM EDW.SYS.PARTITION_FUNCTIONS WHERE NAME = '';

"""


class TSQLPartitionFactory:
    """
    A factory class for generating T-SQL partition functions, schemes, and table creation SQL statements.

    Attributes:
        table_name (str): The name of the table.
        list_columns_definition (list): Column definitions for the table.
        partition_start_value (datetime | int): The start value for partitioning. Default is January 1, 2015.
        partition_end_value (datetime | int): The end value for partitioning. Default is December 1, 2050.
        partition_column_name (str): The name of the column to partition by.
        partition_column_type (str): The data type of the partition column. Default is "datetime".
        partition_interval (Literal["MONTHLY", "DAILY", "WEEKLY", "YEARLY"] | int): The partition interval. Default is "MONTHLY".
        schema_name (str): The schema name for the table. Defaults to extracted schema from table_name if not provided.
    """

    def __init__(self,
                 table_name: str,
                 list_columns_definition: list,

                 partition_start_value: datetime | int = datetime(2015, 1, 1),
                 partition_end_value: datetime | int = datetime(2050, 12, 1),

                 partition_column_name: str = None,
                 partition_column_type: str = "datetime",
                 partition_interval: Literal["MONTHLY", "DAILY", "WEEKLY", "YEARLY"] | int = "MONTHLY",

                 schema_name: str = None,
                 **kwargs):
        """
        Initializes the TSQLPartitionFactory.

        Args:
            table_name (str): The name of the table.
            list_columns_definition (list): Column definitions for the table.
            partition_start_value (datetime | int): Start value for partitioning.
            partition_end_value (datetime | int): End value for partitioning.
            partition_column_name (str): Column to partition by.
            partition_column_type (str): Data type of the partition column. Default is "datetime".
            partition_interval (Literal["MONTHLY", "DAILY", "WEEKLY", "YEARLY"] | int): Partition interval. Default is "MONTHLY".
            schema_name (str): Schema name. Extracted from table_name if not provided.
            **kwargs: Additional unsupported arguments.

        Raises:
            ValueError: If end value is less than start value, schema is not provided, or unsupported arguments are passed.
        """

        if partition_end_value < partition_start_value:
            raise ValueError("partition_end_value must be > partition_start_value")

        if not schema_name:
            if "." not in table_name:
                raise ValueError("table_name should provide explicit schema if schema_name params is not provided")
            else:
                schema_name, table_name = table_name.split(".")

        if kwargs:
            raise ValueError(f"[{self.__class__.__name__}] not support params: {list(kwargs.keys())}")

        self.schema_name = schema_name
        self.table_name = table_name

        self.list_columns_definition = list_columns_definition

        self.partition_start_value = partition_start_value
        self.partition_end_value = partition_end_value

        self.partition_column_name = partition_column_name
        self.partition_column_type = partition_column_type
        self.partition_interval = partition_interval

    def generate_range(self):
        """
        Generates a range of partition values based on the specified interval.

        Returns:
            list: A list of partition boundary values as strings.
        """
        match self.partition_interval:
            case "MONTHLY":
                add = relativedelta(months=1)
            case "WEEKLY":
                add = relativedelta(weeks=1)
            case "DAILY":
                add = relativedelta(days=1)
            case "YEARLY":
                add = relativedelta(years=1)
            case int() as n:
                add = n
            case _:
                raise NotImplementedError(f"[class={self.__class__.__name__}] {self.partition_interval} is not support yet")

        list_date = []
        current_value = self.partition_start_value
        while current_value <= self.partition_end_value:
            list_date += [current_value]
            current_value += add

        if isinstance(self.partition_start_value, datetime):
            list_date = [m.strftime("%Y-%m-%d") for m in list_date]
            if self.partition_column_type == "datetime":
                list_date = [m + " 00:00:00" for m in list_date]
        return list_date

    def get_partition_name(self):
        """
        Generates the name for the partition function.

        Returns:
            str: The name of the partition function.
        """
        PARTITION_NAME = f'PTT_FUNC_{self.partition_interval}_NEW'
        return PARTITION_NAME

    def get_scheme_name(self):
        """
        Generates the name for the partition scheme.

        Returns:
            str: The name of the partition scheme.
        """
        PARTITION_SCHEME_NAME = f'PTT_SCHEME_{self.partition_interval}_NEW'
        return PARTITION_SCHEME_NAME

    def create_partition_functions(self):
        """
        Generates the SQL for creating a partition function.

        Returns:
            str: T-SQL for creating a partition function.
        """
        return f"""
            IF NOT EXISTS (SELECT * FROM {cfg.Core.DB_NAME}.SYS.PARTITION_FUNCTIONS WHERE UPPER(NAME) = '{self.get_partition_name()}')
            CREATE PARTITION FUNCTION {self.get_partition_name()} ({self.partition_column_type})
            AS RANGE RIGHT FOR VALUES ( { ",".join([f"'{m}'" for m in self.generate_range()]) } )
        """

    def create_partition_scheme(self):
        """
        Generates the SQL for creating a partition scheme.

        Returns:
            str: T-SQL for creating a partition scheme.
        """
        return f"""
            IF NOT EXISTS (SELECT * FROM {cfg.Core.DB_NAME}.SYS.PARTITION_SCHEMES WHERE UPPER(NAME) = '{self.get_scheme_name()}')
            CREATE PARTITION SCHEME {self.get_scheme_name()}
            AS PARTITION {self.get_partition_name()}
            ALL TO ('PRIMARY')
        """

    def get_partition_create_table(self):
        """
        Generates the partition clause for table creation.

        Returns:
            str: The partition clause for T-SQL table creation.
        """
        return f" ON {self.get_scheme_name()} ({self.partition_column_name})"

    def create_table_with_partition(self) -> list[str]:
        """
        Generates T-SQL statements for creating a partitioned table.

        Returns:
            list[str]: A list of T-SQL statements for partition function, scheme, and table creation.
        """
        sql_create_table = f"""CREATE TABLE {cfg.Core.DB_NAME}.{self.schema_name}.{self.table_name} ({ ",".join(self.list_columns_definition) })"""
        if self.partition_column_name:
            sql_create_table += self.get_partition_create_table()
            return [self.create_partition_functions(),
                    self.create_partition_scheme(),
                    sql_create_table]
        else:
            return [sql_create_table]


class PartitionManager:
    """
    A manager class for handling SQL Server partition operations.

    Attributes:
        hook: A database connection hook for executing SQL commands.
    """

    def __init__(self, hook):
        """
        Initializes the PartitionManager.

        Args:
            hook: A database connection hook for SQL Server.
        """
        self.hook = hook

    def create_partition_function_and_scheme(self, function_name: str, scheme_name: str, boundaries: list):
        """
        Creates a partition function and scheme in SQL Server.

        Args:
            function_name (str): The name of the partition function.
            scheme_name (str): The name of the partition scheme.
            boundaries (list): Boundary values for the partition function.
        """

        # Create partition function
        boundaries_str = ", ".join([f"'{b}'" for b in boundaries])
        partition_function_sql = f"""
        CREATE PARTITION FUNCTION {function_name} (DATETIME)
        AS RANGE RIGHT FOR VALUES ({boundaries_str});
        """
        self.hook.run_sql(partition_function_sql)

        # Create partition scheme
        partition_scheme_sql = f"""
        CREATE PARTITION SCHEME {scheme_name}
        AS PARTITION {function_name} ALL TO ([PRIMARY]);
        """
        self.hook.run_sql(partition_scheme_sql)

    def identify_partition_number(self, partition_function, some_date):
        """
        Identifies the partition number for a given date using the partition function.

        Args:
            partition_function (str): The partition function name.
            some_date (str): The date to identify the partition number for.

        Returns:
            int: The partition number.
        """
        query = f"""
        SELECT $PARTITION.{partition_function}('{some_date}') AS PARTITION_INDEX_NO;
        """
        result = self.hook.run_sql(query)
        # Extract partition number from the result
        partition_number = result[0]['PARTITION_INDEX_NO']
        return partition_number

    def switch_partition(self, source_table, target_table, partition_number):
        """
        Switches a partition between tables.

        Args:
            source_table (str): The source table.
            target_table (str): The target table.
            partition_number (int): The partition number to switch.

        Steps:
            1. Create a new table with the same schema as the source table.
            2. Switch the partition content from the source table to the new table.
            3. Drop the new table after the switch.
        """
        switch_sql = f"""
        ALTER TABLE {source_table}
        SWITCH PARTITION {partition_number}
        TO {target_table};
        """
        self.hook.run_sql(switch_sql)

        self.hook.run_sql(f"DROP TABLE {target_table}")


if __name__ == '__main__':
    # unit test
    builder_sql = TSQLPartitionFactory(table_name="TBL_TEST_1",
                                       list_columns_definition=[
                                           "col_1 datetime",
                                           "col_2 VARCHAR(100)",
                                       ],
                                       partition_interval="MONTHLY",
                                       partition_column_name="SALES_BY_MONTH",
                                       partition_column_type="datetime",
                                       schema_name="RAW"
                                       )

    for line in builder_sql.create_table_with_partition():
        print(line)
