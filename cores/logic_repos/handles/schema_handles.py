from contextlib import contextmanager

from pyspark.sql.functions import *
from pyspark.sql.types import DataType

from cores.models.transform import PipeTransform
from cores.models.transform import BaseEngine
from cores.engines.spark_engine import SparkEngine


class HandleWithMetadata(PipeTransform):
    """
    Base class for handling transformations that require metadata about the sink.

    Attributes:
        is_request_metadata (bool): Indicates whether metadata is required for the transformation. Default is True.

    Methods:
        - `get_sink_metadata(**kwargs)`: Fetches metadata from the sink.
        - `is_load_to_database(**kwargs)`: Checks if the data is being loaded to a database.
        - `is_sink_exists(**kwargs)`: Verifies if the sink (e.g., table) exists.
        - `is_executable(**kwargs)`: Determines if the transformation can be executed.
        - `is_refresh_full(**kwargs)`: Checks if a full refresh is required based on the ingest or refresh type.
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.is_request_metadata = True

    @staticmethod
    def get_sink_metadata(**kwargs) -> DataFrame:
        """
        Fetches the sink metadata.

        Args:
            **kwargs: Arguments containing metadata and Spark session.

        Returns:
            DataFrame: A DataFrame representing the sink schema and data.
        """
        metadata: SparkEngine = kwargs.get("metadata")
        spark = kwargs.get("spark")
        df_sink = metadata.read_source(source_path=metadata.sink_path,
                                       source_format=metadata.sink_format,
                                       spark=spark,
                                       mssql_conn=metadata.hook_sink.get_connection(metadata.hook_sink.mssql_conn_id))
        return df_sink

    @staticmethod
    def is_load_to_database(**kwargs):
        """
        Determines whether the sink is a database.

        Args:
            **kwargs: Arguments containing metadata.

        Returns:
            bool: True if the sink format is a database, False otherwise.
        """
        metadata: BaseEngine = kwargs.get("metadata")
        if metadata.sink_format in ["mssql", ]: return True
        return False
    
    @staticmethod
    def is_sink_exists(**kwargs):
        """
        Checks if the sink (e.g., table) exists.

        Args:
            **kwargs: Arguments containing metadata.

        Returns:
            bool: True if the sink exists, False otherwise.
        """
        metadata: SparkEngine = kwargs.get("metadata")
        schema_name, table_name = metadata.sink_path.split(".")
        return metadata.hook_sink.is_table_exists(table_name=table_name, schema_name=schema_name)
    
    def is_executable(self, **kwargs):
        """
        Determines if the transformation is executable.

        Args:
            **kwargs: Arguments containing metadata.

        Returns:
            bool: True if executable, False otherwise.
        """
        return (
            self.is_load_to_database(**kwargs) 
            and self.is_sink_exists(**kwargs)
        )

    @staticmethod
    def is_refresh_full(**kwargs):
        """
        Checks if a full refresh is required based on ingest or refresh type.

        Args:
            **kwargs: Arguments containing ingest or refresh type.

        Returns:
            bool: True if a full refresh is required, False otherwise.
        """
        from cores.models.refresh_type import RefreshType, IngestType
        ingest_type = kwargs.get("ingest_type", None)
        refresh_type = kwargs.get("refresh_type", None)

        if ingest_type == IngestType.masterdata: return True
        if refresh_type == RefreshType.full: return True

        return False


class HandleSyncDType(HandleWithMetadata):
    """
    Handles synchronization of data types between the source and sink.

    Methods:
        - `translate_dtype(dtype: DataType)`: Translates a PySpark data type to the sink-compatible type.
        - `__call__(df: DataFrame, **kwargs)`: Synchronizes data types by casting mismatched columns.
    """
    @staticmethod
    def translate_dtype(dtype: DataType):
        """
        Translates a PySpark data type to a compatible sink data type.

        Args:
            dtype (DataType): The PySpark data type.

        Returns:
            DataType: The translated sink-compatible data type.
        """
        result = {
            "BIGINT": "LONG"
        }.get(dtype.simpleString().upper())
        # print(f"[HandleSyncDType.translate_dtype] change {dtype.typeName()=}|{dtype.simpleString()=} to {result=}")
        return result or dtype

    def __call__(self, df: DataFrame, **kwargs):
        """
        Synchronizes data types between the source and sink.

        Args:
            df (DataFrame): The source DataFrame.
            **kwargs: Additional arguments containing metadata.

        Returns:
            DataFrame: The transformed DataFrame with synchronized data types.
        """
        if not self.is_executable(**kwargs): return df

        df_sink = self.get_sink_metadata(**kwargs)
        cast_type_cols = {field.name: col(field.name).cast(self.translate_dtype(field.dataType))
                          for field in df_sink.schema.fields
                          if field.dataType.typeName() != dict(df.dtypes)[field.name]}
        for k, v in cast_type_cols.items():
            print(f"[HandleSyncDType] {k}: {v}")
        return df.withColumns(cast_type_cols)


class HandleMissingColumn(HandleWithMetadata):
    """
    Handles missing columns by adding them to the DataFrame with null values.

    Methods:
        - `__call__(df: DataFrame, **kwargs)`: Adds missing columns from the sink schema to the DataFrame.
    """

    def __call__(self, df: DataFrame, **kwargs):
        """
        Adds missing columns to the DataFrame based on the sink schema.

        Args:
            df (DataFrame): The source DataFrame.
            **kwargs: Additional arguments containing metadata.

        Returns:
            DataFrame: The transformed DataFrame with added columns.
        """
        if not self.is_executable(**kwargs): return df

        df_sink = self.get_sink_metadata(**kwargs)
        source_columns = df.columns
        additional_cols = {field.name: lit(None).cast(field.dataType)
                           for field in df_sink.schema.fields
                           if field.name not in source_columns}
        print(f"[HandleMissingColumn] {additional_cols=}")
        return df.withColumns(additional_cols)


class HandleNullableColumn(HandleWithMetadata):
    """
    Ensures column nullability matches between the source and sink.

    Methods:
        - `__call__(df: DataFrame, **kwargs)`: Adjusts nullability for columns in the DataFrame.
    """
    def __call__(self, df: DataFrame, **kwargs):
        """
        Adjusts column nullability in the DataFrame to match the sink schema.

        Args:
            df (DataFrame): The source DataFrame.
            **kwargs: Additional arguments containing metadata.

        Returns:
            DataFrame: The transformed DataFrame with adjusted nullability.
        """
        if not self.is_executable(**kwargs): return df

        df_sink = self.get_sink_metadata(**kwargs)
        source_columns = df.columns
        sink_columns = df_sink.columns
        list_columns = set.intersection(set(source_columns), set(sink_columns))
        for m in list_columns:
            if df.schema[m].nullable != df_sink.schema[m].nullable:
                print(f"[HandleNullableColumn] column {m}: {df.schema[m].nullable == df_sink.schema[m].nullable}")
                df.schema[m].nullable = df_sink.schema[m].nullable
        return df


class _HandleNewColumnIGNORE(HandleWithMetadata):
    """
    Ignores new columns in the source that do not exist in the sink.

    Methods:
        - `__call__(df: DataFrame, **kwargs)`: Drops new columns from the DataFrame.
    """
    def __call__(self, df: DataFrame, **kwargs):
        """
        Drops new columns that do not exist in the sink schema.

        Args:
            df (DataFrame): The source DataFrame.
            **kwargs: Additional arguments containing metadata.

        Returns:
            DataFrame: The transformed DataFrame without new columns.
        """
        if (not self.is_executable(**kwargs)
            or self.is_refresh_full(**kwargs)): return df

        df_sink = self.get_sink_metadata(**kwargs)
        source_columns = df.columns
        sink_columns = df_sink.columns
        new_columns = [m for m in source_columns if m not in sink_columns]
        print(f"[HandleNewColumnIGNORE] {new_columns=}")
        return df.drop(*new_columns)


class _HandleNewColumnADD(HandleWithMetadata):
    """
    Adds new columns from the source to the sink schema.

    Methods:
        - `__call__(df: DataFrame, **kwargs)`: Adds new columns to the sink schema.
    """

    # @staticmethod
    # @contextmanager
    # def create_temp_blank_table(df: DataFrame, metadata: SparkEngine, **kwargs):
    #     from uuid import uuid4
    #     from copy import deepcopy
    #
    #     schema_name, table_name = metadata.sink_path.split(".")
    #     temp_table_name = f"{schema_name}.TEMP_{schema_name}_{table_name}_{uuid4().int}"
    #
    #     spark_options = deepcopy(metadata.default_options)
    #     spark_options["dbtable"] = temp_table_name
    #
    #     try:
    #         # create temp table at sink
    #         from cores.models.refresh_type import RefreshBehavior
    #         RefreshBehavior.process_create_empty_table(df=df, default_options=spark_options)
    #         hook_sink = metadata.hook_sink
    #         df_schema = hook_sink.get_table_scheme(table_name=temp_table_name).reset_index()
    #         yield df_schema
    #     finally:
    #         del df_schema
    #         metadata.hook_source.run_sql(f"DROP TABLE IF EXISTS {temp_table_name}")

    # def __call__(self, df: DataFrame, **kwargs):
    #     if not self.is_executable(**kwargs): return df
    #
    #     df_sink = self.get_sink_metadata(**kwargs)
    #     source_columns = df.columns
    #     sink_columns = df_sink.columns
    #     new_columns = [m for m in source_columns if m not in sink_columns]
    #     if new_columns:
    #         sink_path = kwargs.get("metadata").sink_path
    #         hook_sink = kwargs.get("metadata").hook_sink
    #
    #         with self.create_temp_blank_table(df=df, **kwargs) as df_schema:
    #             for column_name in new_columns:
    #                 dtype = df_schema[df_schema["COL_NAME"].eq(column_name)]["DATA_TYPE"].tolist()[0]
    #                 match str(dtype).upper():
    #                     case "VARCHAR" | "NVARCHAR":
    #                         dtype = dtype + "(1000)"
    #                     case 'SMALLDECIMAL' | 'DECIMAL':
    #                         dtype = "NUMERIC(30,8)"
    #                 sql = f"ALTER TABLE {sink_path} ADD {column_name} {dtype} NULL"
    #                 hook_sink.run_sql(sql)
    #                 print(f"[HandleNewColumnADD] {sql=}")
    #
    #     return df

    def __call__(self, df: DataFrame, **kwargs):
        """
        Adds new columns from the source to the sink schema.

        Args:
            df (DataFrame): The source DataFrame.
            **kwargs: Additional arguments containing metadata.

        Returns:
            DataFrame: The transformed DataFrame.
        """
        if not self.is_executable(**kwargs): return df

        df_sink = self.get_sink_metadata(**kwargs)
        source_columns = df.columns
        sink_columns = df_sink.columns
        new_columns = [m for m in source_columns if m not in sink_columns]

        source_dtypes = dict(df.dtypes)
        # pyspark dtypes --------------------------------------------
        # Integer: tinyint, smallint, int, bigint (long)
        # numeric: double, decimal(%d,%d)
        # String: char(%d), varchar(%d), string
        # datetime: interval, timestamp,
        # special: array<%s>, map<%s,%s>, %s:%s (struct), struct<%s>
        # -----------------------------------------------------------

        if new_columns:
            sink_path = kwargs.get("metadata").sink_path
            hook_sink = kwargs.get("metadata").hook_sink

            for column_name in new_columns:
                dtype = source_dtypes[column_name].lower()
                match dtype:
                    # integer:
                    case "tinyint" | "smallint" | "int":
                        dtype = "int"
                    case "bigint":
                        ...

                    # numeric:
                    case 'float':
                        ...
                    case 'double' | 'numeric':
                        dtype = "numeric(30,8)"
                    case dtype if "decimal" in dtype:
                        dtype = "numeric(30,8)"

                    # String:
                    case "string" | "text" | "str":
                        dtype = "varchar(1000)"
                    case dtype if "char" in dtype:
                        dtype = "varchar(1000)"

                    # datetime
                    case "date" | "datetime" | "timestamp_ntz" | "timestamp":
                        dtype = "datetime"
                    case dtype if "interval" in dtype:
                        dtype = "varchar(50)"

                    # else
                    case _:
                        dtype = "varchar(1000)"

                sql = f"ALTER TABLE {sink_path} ADD {column_name} {dtype} NULL"

                print(f"[HandleNewColumnADD] {sql=}")
                hook_sink.run_sql(sql)

        return df


class HandleNewColumn:
    """
    A wrapper class for handling new columns in different ways.

    Attributes:
        IGNORE: Class for ignoring new columns.
        ADD: Class for adding new columns to the sink schema.
    """
    IGNORE = _HandleNewColumnIGNORE
    ADD = _HandleNewColumnADD
