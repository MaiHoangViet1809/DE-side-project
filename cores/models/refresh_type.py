from enum import Enum, EnumMeta
from typing import cast, Literal

from pyspark.sql import DataFrame as SparkDF

"""
This module defines enums and a base class for handling data ingestion and refresh behaviors
in a Spark pipeline, specifically designed for SQL Server integration.
"""


class IngestType(Enum):
    """
    Enum representing types of data ingestion.
    """
    incremental = "INCREMENTAL"
    mtd = "MTD"
    back_fill_n_days = "BACK_FILL_N_DAYS"
    snapshot = "SNAPSHOT"
    masterdata = "MASTERDATA"
    cdc = "CDC"


class RefreshType(Enum):
    """
    Enum representing types of data refresh.
    """
    full = "FULL"
    partition = "PARTITION"
    partition_mtd = "PARTITION_MTD"
    partition_n_days = "PARTITION_N_DAYS"


class RefreshBehavior:
    """
    Base class defining how data is refreshed in a pipeline.

    Args:
        sink (str): Target table or sink path.
        hook_sink: Database connection hook for executing queries.
    """

    def __init__(self,
                 sink: str,
                 hook_sink = None):
        self.sink = sink
        self.hook_sink = hook_sink
        self.partition_condition: str = ""
        self.sink_version = self.get_version()
        print(f"[RefreshBehavior] current SQL Server version in Sink {self.sink_version=}")

    def get_version(self):
        """
        Get the version of SQL Server.

        Returns:
            int: Major version of SQL Server.
        """
        from pandas import DataFrame
        data : DataFrame = self.hook_sink.get_pandas_df("SELECT CAST(SERVERPROPERTY('productversion') AS VARCHAR) AS PRODUCT_VERSION")

        if data.size > 0:
            version = data["PRODUCT_VERSION"].iloc[0]
            return int(version.split(".")[0])

        return 0

    def process_delete_data(self):
        """
        Delete data from the sink location or table based on partition conditions.
        """
        from contextlib import closing
        from pyodbc import SQL_ATTR_TXN_ISOLATION, SQL_TXN_READ_COMMITTED

        sql = f"""DELETE FROM {self.sink} {self.partition_condition}"""
        # self.hook_sink.run_sql(sql)

        print(f"[RefreshBehavior.process_delete_data] running: {sql=}")
        with closing(self.hook_sink.get_conn()) as conn:
            conn.set_attr(SQL_ATTR_TXN_ISOLATION, SQL_TXN_READ_COMMITTED)

            if self.hook_sink.supports_autocommit:
                self.hook_sink.set_autocommit(conn, True)

            with closing(conn.cursor()) as cur:
                cur.execute(sql)
                if cur.rowcount >= 0: print("Rows affected: %s", cur.rowcount)

            if not self.hook_sink.get_autocommit(conn):
                conn.commit()

    def process_load_ndays(self, df: SparkDF, partition_column, partition_value, **kwargs):
        """
        Process and delete data for N-day partitions.
        """
        from pyspark.sql.functions import col, min, max, date_format
        assert partition_column is not None, "partition_column argument is missing"

        partition_n_days = kwargs.get("partition_n_days")
        if not partition_n_days:
            lower_bound = df.agg(date_format(min(col(partition_column)), 'yyyy-MM-dd').alias("LB")).collect()[0]["LB"]
            upper_bound = df.agg(date_format(max(col(partition_column)), 'yyyy-MM-dd').alias("UB")).collect()[0]["UB"]
            self.partition_condition = f"""
                WHERE {partition_column} BETWEEN CAST(CAST('{lower_bound}' AS DATE) AS DATETIME)
                                             AND CAST(CAST('{upper_bound}' AS DATE) AS DATETIME) + 1 - 1.0/60/60/24
            """

        else:
            self.partition_condition = f"""WHERE {partition_column} BETWEEN CAST('{partition_value}' AS DATETIME) - {partition_n_days}
                                                                     AND CAST('{partition_value}' AS DATETIME) + 1 - 1.0/60/60/24
                                        """
        self.process_delete_data()

    def process_load_mtd(self, partition_column, partition_value):
        """
        Process and delete data for month-to-date (MTD) partitions.
        """
        self.partition_condition = f"""WHERE {partition_column} BETWEEN CAST(EOMONTH('{partition_value}', -1) AS DATETIME) + 1 
                                                                 AND CAST(EOMONTH('{partition_value}',  0) AS DATETIME) + 1 - 1.0/60/60/24
                                    """
        self.process_delete_data()

    def execute(self, df: SparkDF, **kwargs):
        """
        Execute the data refresh pipeline.

        Args:
            df (SparkDF): Input PySpark DataFrame.

        Kwargs:
            ingest_type: in used: CDC/MASTERDATA/BACK_FILL_N_DAYS - not used: MTD/INCREMENTAL/SNAPSHOT
            file_name: path to file to ingest/load

            refresh_type: FULL/PARTITION/PARTITION_MTD/PARTITION_N_DAYS

            partition_column: column name using to partition
            partition_value: value to delete data from partition and filter from sourcing

            default_options: (advance - change with caution) default options using in spark createTableOptions
            n_partitions: (default 1 - do not use this params) internal number of partition - repartition data (on spark) to match clustered column store (on SQL Server)

            partition_interval: (default 'Monthly' - do not use this params) partition interval (on sink)
            partition_column_type: (default 'datetime' - do not use this params) column partition datatype

        Returns:
            SparkDF: Processed DataFrame.
        """
        partition_column = kwargs.get("partition_column")
        partition_value = kwargs.get("partition_value")
        
        if not self.check_sink_exist():
            df = self.process_drop_and_create_table(df=df, **kwargs)
            
        elif "refresh_type" in kwargs:
            # check refresh type
            refresh_type = kwargs.get("refresh_type")
            if isinstance(refresh_type, str): refresh_type = RefreshType(refresh_type)

            match refresh_type:
                case RefreshType.partition:
                    self.partition_condition = f" WHERE {partition_column} = '{partition_value}'"
                    self.process_delete_data()
                case RefreshType.partition_mtd:
                    self.process_load_mtd(partition_column=partition_column, partition_value=partition_value)
                case RefreshType.partition_n_days:
                    self.process_load_ndays(df=df, partition_column=partition_column, partition_value=partition_value)
                case RefreshType.full:
                    df = self.process_drop_and_create_table(df=df, **kwargs)

        elif "ingest_type" in kwargs:
            match kwargs.get("ingest_type"):
                case IngestType.cdc:
                    file_name = kwargs.get("file_name")
                    if not file_name:
                        # if file_name not provided, query it from source dataframe
                        if "FILE_NAME" in df.columns:
                            file_name = [row.FILE_NAME for row in df.select("FILE_NAME").distinct().collect()]

                    if isinstance(file_name, str):
                        file_name = [file_name]

                    if file_name:
                        schema, table_name = self.sink.split(".")
                        df_schema = self.hook_sink.get_table_scheme(table_name=table_name, schema=schema).reset_index()

                        is_exist_filename = "FILENAME" in df_schema["COL_NAME"].str.upper().values.tolist()
                        is_exist_file_name = "FILE_NAME" in df_schema["COL_NAME"].str.upper().values.tolist()

                        sql = None
                        file_name_string_query = ', '.join([f"'{m}'" for m in file_name])
                        match is_exist_filename, is_exist_file_name:
                            case True, True:
                                sql = f"WHERE FILENAME IN ({file_name_string_query}) OR FILE_NAME IN ({file_name_string_query})"
                            case _, True:
                                sql = f"WHERE FILE_NAME IN ({file_name_string_query})"
                            case True, _:
                                sql = f"WHERE FILENAME IN ({file_name_string_query})"

                        if sql:
                            self.partition_condition = sql
                            self.process_delete_data()

                case IngestType.back_fill_n_days:
                    self.process_load_ndays(df=df, partition_column=partition_column, partition_value=partition_value)

                case IngestType.mtd:
                    self.process_load_mtd(partition_column=partition_column, partition_value=partition_value)

                case IngestType.masterdata:
                    df = self.process_drop_and_create_table(df=df, **kwargs)

        return df

    def process_drop_and_create_table(self, df: SparkDF, **kwargs):
        """
        Drop the existing table (if it exists) and create a new one based on the given DataFrame schema.

        Args:
            df (SparkDF): The PySpark DataFrame to use for schema definition.
            **kwargs: Additional keyword arguments for configuration.

        Returns:
            SparkDF: The processed DataFrame.
        """
        n_partitions = kwargs.get("n_partitions", 1)
        default_options = kwargs.get("default_options")

        create_table_option = self.process_create_partition(**kwargs)
        default_options["createTableOptions"] = create_table_option
        self.process_create_empty_table(df=df, default_options=default_options)
        df = self.process_create_column_storage_mssql(df=df, n_partitions=n_partitions, create_table_option=create_table_option)
        return df

    def check_sink_exist(self):
        """
        Check if the target sink (table) exists.

        Returns:
            bool: True if the sink exists, False otherwise.
        """
        # handle source
        schema_name, table_name = self.sink.split(".")
        return self.hook_sink.is_table_exists(table_name=table_name, schema_name=schema_name)

    @staticmethod
    def process_create_empty_table(df: SparkDF, default_options: dict):
        """
        Create an empty table with the schema of the given DataFrame.

        Args:
            df (SparkDF): The PySpark DataFrame defining the schema.
            default_options (dict): Options for table creation.
        """
        (df.where("1=0")
         .write
         .mode("overwrite")
         .format("com.microsoft.sqlserver.jdbc.spark")
         .options(**default_options)
         .save())

    def process_create_partition(self, **kwargs):
        """
        Create partition definitions for the table if specified.

        Args:
            **kwargs: Additional keyword arguments for configuration.

        Returns:
            str: The partition-specific create table options.
        """
        # create partition
        create_table_option = ""
        partition_column = kwargs.get("partition_column")
        if partition_column == "None": partition_column = None

        if partition_column is not None:
            from cores.models.managers.mssql_partition import TSQLPartitionFactory
            partition_builder = TSQLPartitionFactory(table_name=self.sink,
                                                     list_columns_definition=[],
                                                     partition_interval=kwargs.get("partition_interval", cast(Literal, "MONTHLY")),
                                                     partition_column_name=partition_column,
                                                     partition_column_type=kwargs.get("partition_column_type", "datetime")
                                                     )

            # only create partition in UAT environment, production environment version 2016 too low and not supported
            if self.sink_version >= 14:
                self.hook_sink.run_sql(partition_builder.create_partition_functions())
                self.hook_sink.run_sql(partition_builder.create_partition_scheme())

            create_table_option = partition_builder.get_partition_create_table()

        return create_table_option
    
    def process_create_column_storage_mssql(self, df: SparkDF, n_partitions: int, create_table_option: str):
        """
        Create a clustered columnstore index for the table and optimize partitioning.

        Args:
            df (SparkDF): The PySpark DataFrame to process.
            n_partitions (int): Number of partitions for the DataFrame.
            create_table_option (str): Create table options.

        Returns:
            SparkDF: The repartitioned DataFrame.
        """
        # only create columns stored in UAT environment, production environment version 2016 too low and not supported
        if self.sink_version >= 14:
            # check column datatype not supported with CCI table
            from cores.utils.debug import print_table
            df_schema = self.hook_sink.get_table_scheme(self.sink)
            df_adj = df_schema[df_schema["MAX_LENGTH"].eq(-1) & df_schema["DATA_TYPE"].isin(["nvarchar", "varchar"])].reset_index()

            if df_adj.shape[0] > 0:
                print("[write_sink] found columns not supported for CCI table - process to rebuild table with compatibility datatype")

                # TODO: calculate max length
                # df = df.select([max(length(col(name))).alias(name) for name in df.schema.names])
                # row=df.first().asDict()
                # df2 = spark.createDataFrame([Row(col=name, length=row[name]) for name in df.schema.names], ['col', 'length'])

                from numpy import where
                df_adj["SQL_DDL"] = "ALTER COLUMN " + df_adj["COL_NAME"] + " " + df_adj["DATA_TYPE"] + "(1000) " + where(df_adj["IS_NULLABLE"].eq(True), "NULL", "NOT NULL")
                print_table(df_adj[["COL_NAME", "DATA_TYPE", "MAX_LENGTH"]], 100)
                list_cols = [f"ALTER TABLE {self.sink} {m}" for m in df_adj["SQL_DDL"].to_list()]
                for m in list_cols:
                    self.hook_sink.run_sql(m)

            print("[write_sink] start create clustered columnstore index")
            sql = f"CREATE CLUSTERED COLUMNSTORE INDEX CCI_{self.sink.replace('.', '_')} ON {self.sink}"
            sql += " " + create_table_option
            self.hook_sink.run_sql(sql)

        # repartition data to match clustered column store table row_group max performance
        if n_partitions:
            print("[write_sink] repartition data to match clustered column store")
            df = df.repartition(numPartitions=n_partitions)

        return df


if __name__ == "__main__":
    print(type(RefreshType))
    print(isinstance(RefreshType, EnumMeta))
    print(RefreshType("PARTITION_MTD"))
    print(isinstance(RefreshType("PARTITION_MTD"), Enum))
    print(isinstance(IngestType.masterdata, IngestType))
    print(RefreshType.full == RefreshType("FULL"))
