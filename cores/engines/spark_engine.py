from typing import Callable  # cast, Literal,
from pyspark.sql import SparkSession, DataFrame as SparkDF
from contextlib import contextmanager
from pandas import DataFrame

from cores.models.transform import BaseEngine
from cores.models.refresh_type import RefreshBehavior

from cores.hooks.sparks import SparkHook

from cores.utils.providers import ProvideBenchmark
from cores.utils.shells import run_sh

from cores.logic_repos.handles.naming_convension import HandleColumnNamingConvention
from cores.logic_repos.handles.write_to_temp_file import CTXHandleCacheDataFromDB


@contextmanager
def xlsb_to_xlsx(file_name: str, data_address: str):
    """
    Converts an XLSB file to XLSX format or directly uses an XLSX file.

    Args:
        file_name (str): Path to the XLSB or XLSX file.
        data_address (str): Data address to process within the file.

    Yields:
        str: Path to the converted or original XLSX file.
    """
    from cores.utils.excels import convert_xlsb_xlsx
    new_file = None
    if file_name.lower().endswith(".xlsb"):
        try:
            new_file = convert_xlsb_xlsx(file_name=file_name, data_address=data_address)
            yield new_file
        finally:
            ...
            # from pathlib import Path
            # Path(new_file).unlink()
    else:
        yield file_name


def df_union_by_name(list_dfs: list[SparkDF]):
    """
    Union multiple DataFrames by aligning columns by name.

    Args:
        list_dfs (list[SparkDF]): List of Spark DataFrames to union.

    Returns:
        SparkDF: A single Spark DataFrame containing all input DataFrames unioned by name.
    """
    from functools import reduce, partial
    union_by_name = partial(SparkDF.unionByName, allowMissingColumns=True)
    return reduce(union_by_name, list_dfs)


class SparkEngine(BaseEngine):
    """
    A class to manage data transformations and ingestion using Spark.

    This class supports multiple data formats for both source and sink and provides tools for schema evolution,
    custom transformations, and integration with Spark.

    Args:
        source_path (str | list[str]): Path or list of paths to the data source.
        source_format (str): Format of the source data (e.g., "csv", "parquet").
        sink_path (str, optional): Path to the data sink.
        sink_format (str, optional): Format of the data sink (e.g., "mssql", "csv").
        framework_config: Framework-specific configurations, if any.
    """

    def __init__(self, source_path: str | list[str], source_format: str,  sink_path: str = None, sink_format: str = None, framework_config = None):
        """
        Initializes the SparkEngine with source and sink configurations.

        Args:
            source_path (str | list[str]): Path or list of paths to the data source.
            source_format (str): Format of the source data (e.g., "csv", "parquet", "mssql").
            sink_path (str, optional): Path to the data sink.
            sink_format (str, optional): Format of the data sink (e.g., "csv", "parquet", "mssql").
            framework_config (optional): Framework-specific configurations, if applicable.
        """
        super().__init__(source_path=source_path, source_format=source_format,
                         sink_path=sink_path, sink_format=sink_format)
        self.old_source_path = source_path
        self.hook_source = None
        self.hook_sink = None
        self.default_options = None

        list_jars = [
            "com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA",
            "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre8"
        ]  #
        # list_jars += ["com.crealytics:spark-excel_2.12:3.3.2_0.19.0"] if self.source_format.lower() in ["excel", "xlsb", "xlsx"] else []
        list_jars += ["com.crealytics:spark-excel_2.12:3.3.3_0.20.2"] if self.source_format.lower() in ["excel", "xlsb", "xlsx"] else []

        self.spark_hook = SparkHook(jars=list_jars)

        # self.spark_info: dict = {}

        self.framework_config = framework_config

        self.internal_logs: list[str] = []

    def add_log(self, msg: str):
        self.internal_logs.append(msg)

    def jdbc_connection_string(self, conn, hook):
        """
        Constructs a JDBC connection string for the source or sink.

        Args:
            conn: Connection object.
            hook: Hook object for the source or sink.

        Returns:
            str: JDBC connection string.
        """
        if hook:
            db_name = hook.database or conn.schema or "EDW"
        else:
            db_name = conn.schema or "EDW"

        if self.framework_config:
            db_name = self.framework_config.Core.DB_NAME
        return f"jdbc:sqlserver://{conn.host}:{conn.port};databaseName={db_name};"

    def set_default_options(self, **kwargs):
        """
        Sets default options for writing to the sink.

        Args:
            kwargs: Additional configurations for the sink.
        """
        if self.sink_format == "mssql":
            # handle sink default_options
            mssql_conn = self.hook_sink.get_connection(self.hook_sink.mssql_conn_id)  # noqa: mssql_conn_id - dynamic getattr in super class
            url = self.jdbc_connection_string(mssql_conn, hook=self.hook_sink)

            self.default_options = dict(
                dbtable=self.sink_path,
                Driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                url=url,
                user=mssql_conn.login,
                password=mssql_conn.password,
                trustServerCertificate=True,

                batchsize=kwargs.get("batchsize", 1048576),
                tableLock=kwargs.get("tableLock", True),  # revert to true
                numPartitions=kwargs.get("executor_cores", 4),

                # truncate=True
                schemaCheckEnabled=False,
            )

    def set_write_mode(self, **kwargs):
        """
        Determines the write mode for the sink.

        Args:
            kwargs: Additional configurations for the sink.

        Returns:
            dict: Updated configurations including write mode.
        """
        if self.sink_format == "mssql":
            # handle source
            schema_name, table_name = self.sink_path.split(".")
            if not self.hook_sink.is_table_exists(table_name=table_name, schema_name=schema_name):
                # if table in sink not exist then using overwrite mode to create table
                kwargs["write_mode"] = "overwrite"
        return kwargs

    def process_apply_transforms(self, df: SparkDF, spark, **kwargs):
        """
        Applies transformation functions to the given DataFrame.

        Args:
            df (SparkDF): Input Spark DataFrame.
            spark (SparkSession): Active Spark session.
            kwargs: Additional configurations for transformations.

        Returns:
            SparkDF: Transformed DataFrame.
        """
        apply_transform_func = kwargs.get("apply_transform_func")
        if apply_transform_func:
            self.msg("[process_transform_function] start apply transform function")
            self.msg("-" * 50)

            if isinstance(apply_transform_func, Callable):
                self.msg(f"[process_transform_function] apply function {apply_transform_func}")
                df = apply_transform_func(df)
            elif isinstance(apply_transform_func, list):
                for c in apply_transform_func:
                    self.msg(f"[process_transform_function] apply function {c}")
                    if getattr(c, "is_request_metadata", False):
                        df = c(df, metadata=self, spark=spark)
                    else:
                        df = c(df)

            self.msg("-" * 50)
        return df

    def pre_process(self, spark: SparkSession, df_source: SparkDF, **kwargs):
        """
        Prepares the source DataFrame for further processing and writing.

        Args:
            spark (SparkSession): Active Spark session.
            df_source (SparkDF): Source Spark DataFrame.
            kwargs: Additional configurations.

        Returns:
            tuple: Transformed DataFrame and updated configurations.
        """
        # set write mode
        kwargs = self.set_write_mode(**kwargs)

        # set default_options
        self.set_default_options(**kwargs)

        # force naming convention
        df_source = HandleColumnNamingConvention()(df=df_source)

        # print dataframe before transformation
        self.msg("------------------------ dataframe before transformation -----------------------------")
        df_source.show(2)

        # apply default handlers:
        from cores.logic_repos.handles.schema_handles import HandleMissingColumn, HandleNullableColumn, HandleNewColumn, HandleSyncDType
        df_source = HandleMissingColumn()(df=df_source, metadata=self, spark=spark, **kwargs)
        schema_evolution = kwargs.get('schema_evolution', False)
        if not schema_evolution:
            df_source = HandleNewColumn.IGNORE()(df=df_source, metadata=self, spark=spark, **kwargs)
        else:
            df_source = HandleNewColumn.ADD()(df=df_source, metadata=self, spark=spark, **kwargs)

        df_source = HandleNullableColumn()(df=df_source, metadata=self, spark=spark, **kwargs)

        # apply callable transform:
        df_source = self.process_apply_transforms(df=df_source, spark=spark, **kwargs)

        # sync datatype between source and sink
        df_source = HandleSyncDType()(df=df_source, metadata=self, spark=spark)

        # print dataframe after transformation
        self.msg("------------------------ dataframe after transformation -----------------------------")
        df_source.show(2)

        return df_source, kwargs

    @ProvideBenchmark  # noqa
    def transform(self, hook = None, hook_sink = None, **kwargs):
        """
        Transforms the source data and writes it to the sink.

        Args:
            hook: Source hook for accessing the data.
            hook_sink: Sink hook for writing the transformed data.
            kwargs: Additional configurations.

        Returns:
            int: Number of rows transformed and written.
        """
        self.hook_source = hook
        self.hook_sink = hook_sink

        executor_cores = kwargs.get("executor_cores", 2)

        self.spark_hook.spark_config.set_spark_memory(kwargs.pop("memory", "2G"))
        self.spark_hook.spark_config.set_master(kwargs.pop("master", f"local[{executor_cores}]"))

        # get list of spark config from kwargs
        list_config = self.spark_hook.get_list_configs()
        for m in list_config:
            if m in kwargs:
                config_value = kwargs.pop(m)
                self.spark_hook.spark_config[m] = config_value

        # deal with CTE in source_format = mssql
        # with self.create_view_for_cte() as adjust_source, self.spark_hook.spark as spark, self.get_spark_info(spark):
        with self.create_view_for_cte() as adjust_source, self.spark_hook.spark as spark:
            self.old_source_path = self.source_path
            self.source_path = adjust_source

            df_source = self.read_source(source_path=self.formatted_input,
                                         source_format=self.source_format,
                                         spark=spark,
                                         mssql_conn=self.hook_source.get_connection(self.hook_source.mssql_conn_id) if self.hook_source else None,
                                         **kwargs)

            # apply pre-processing first then cache later
            df_source, kwargs = self.pre_process(spark=spark, df_source=df_source, **kwargs)

            # speedup process by write to temp file parquet first
            with CTXHandleCacheDataFromDB()(df=df_source, metadata=self, spark=spark, **kwargs) as df_output:
                df_source = df_output

                n_rows = df_source.count()
                n_partitions = max((n_rows // 1048576) + 1, 1)
                self.msg(f"[transform] total-rows:{n_rows} n_partitions: current={df_source.rdd.getNumPartitions()} suggest={n_partitions}")

                self.write_sink(df_source, n_partitions=n_partitions, **kwargs)

                self.source_path = self.old_source_path

                return n_rows

    # @contextmanager
    # def get_spark_info(self, spark: SparkSession):
    #     try:
    #         self.spark_info["web-url"] = spark.sparkContext.uiWebUrl
    #         self.spark_info["application-id"] = spark.sparkContext.statusTracker()
    #         yield
    #     finally:
    #         self.spark_info = {}

    @contextmanager
    def create_view_for_cte(self):
        """
        Handles the creation of temporary views for Common Table Expressions (CTEs).

        Yields:
            str: SQL string for use in transformations.
        """
        from uuid import uuid4
        if self.sink_format.lower() in ["mssql", "tsql"]:
            if not isinstance(self.source_path, str):
                yield self.source_path
            else:
                if "WITH " in self.source_path.upper() or "WITH\n" in self.source_path.upper():
                    schema_name, table_name = self.sink_path.split(".")
                    view_name = f"STG.VW_{schema_name}_{table_name}_{uuid4().int}"

                    try:
                        # create temp view at source
                        self.hook_source.run_sql(f"CREATE VIEW {view_name} AS {self.source_path}", log_sql=True)  # OR ALTER
                        yield f"SELECT * FROM {view_name}"
                    finally:
                        self.hook_source.run_sql(f"DROP VIEW IF EXISTS {view_name}")
                else:
                    yield self.source_path
        else:
            yield self.source_path

    def write_sink(self, df: SparkDF, apply_transform_func=None, write_mode: str = "append", n_partitions=None, **kwargs):
        """
        Writes the DataFrame to the specified sink using the appropriate format.

        Args:
            df (SparkDF): Spark DataFrame to write.
            apply_transform_func: Optional function for further transformations before writing.
            write_mode (str): Write mode (e.g., "append", "overwrite").
            n_partitions: Number of partitions for the DataFrame.
            kwargs: Additional configurations.
        """
        self.msg(f"[write_sink] write to {self.sink_format}")
        if self.sink_format == "mssql":
            if truncate := kwargs.pop("truncate", False): self.default_options["truncate"] = truncate

            behavior = RefreshBehavior(sink=self.sink_path, hook_sink=self.hook_sink)
            df = behavior.execute(df=df, n_partitions=n_partitions, default_options=self.default_options, **kwargs)

            self.msg(f"[write_sink] start write data {write_mode=}")
            (df.write.mode("append")
             .format("com.microsoft.sqlserver.jdbc.spark")
             .options(**self.default_options)
             .option("isolationLevel", "READ_COMMITTED")
             .save())

        elif self.sink_format == "parquet":
            df.write.mode(write_mode).options(**kwargs).parquet(self.sink_path)

        elif self.sink_format == "csv":
            assert ".csv" in self.sink_path.lower(), "name of csv file should be included in sink_path !!"
            df.toPandas().to_csv(self.sink_path,
                                 index=False,
                                 sep=kwargs.get("delimiter", "|"),
                                 header=True,
                                 encoding="utf-8",
                                 lineterminator="\r\n",  # update change to CRLF for windows compatibility
                                 )
            # debug
            from cores.utils.shells import run_sh
            rtcode, stdout = run_sh(f"tail -n 1 {self.sink_path}")
            print("[write_sink] last line:", stdout)

    def read_source(self, source_path: str | list[str], source_format: str, spark: SparkSession, mssql_conn, **kwargs) -> SparkDF:
        """
        Reads the source data into a Spark DataFrame.

        Args:
            source_path (str | list[str]): Path(s) to the data source.
            source_format (str): Format of the source data.
            spark (SparkSession): Active Spark session.
            mssql_conn: MSSQL connection object.
            kwargs: Additional configurations.

        Returns:
            SparkDF: Source data as a Spark DataFrame.
        """
        self.msg("[read_source] start")
        if isinstance(source_path, list):
            list_dfs = []
            for m in source_path:
                list_dfs.append(getattr(self, f"_read_{source_format}")(source=m, spark=spark, conn=mssql_conn, **kwargs))
            df_source = df_union_by_name(list_dfs)
        else:
            df_source = getattr(self, f"_read_{source_format}")(source=source_path, spark=spark, conn=mssql_conn, **kwargs)
        return df_source

    @property
    def formatted_input(self):
        """
        Formats the input path based on the source format.

        Returns:
            str: Formatted input path.
        """
        match self.source_format:
            case "csv" | "excel" | "parquet":
                if self.source_path.startswith("s3a://") or self.source_path.startswith("file://"):
                    return_path = self.source_path
                else:
                    return_path = f"file://{self.source_path}"

                return return_path
            case "mssql" | _:
                return self.source_path

    def _read_csv(self, source: str | list[str], spark: SparkSession, **kwargs) -> SparkDF:
        """
        Reads data from CSV files.

        Args:
            source (str | list[str]): Path(s) to the CSV files.
            spark (SparkSession): Active Spark session.
            kwargs: Additional configurations.

        Returns:
            SparkDF: Data from CSV files as a Spark DataFrame.
        """
        from cores.utils.encoding import predict_encoding

        source_file_path = source.removeprefix('file://')
        encoding = predict_encoding(source_file_path, n_lines=10000)
        rtcode, shell_encoding_check = run_sh(command=f"""file -bi "{source_file_path}" """, stream_stdout=False, return_log=True)
        print(f"[_read_csv] detected {encoding=} {shell_encoding_check=}")

        # detect delimiter
        default_delimiter = self.framework_config.Ingestion.TEXT_DELIMITER if self.framework_config else "|"
        delimiter = kwargs.get("delimiter", default_delimiter)

        auto_detect_delimiter = kwargs.get("auto_delimiter", False)
        if auto_detect_delimiter:
            with open(source_file_path, mode="r", encoding=encoding) as file:
                first_line = file.readline()

            n_current = len(first_line.split(delimiter)) - 1
            n_default = len(first_line.split(default_delimiter)) - 1
            n_fallbacks = len(first_line.split(",")) - 1

            if n_current == 0:
                if n_default > 0:
                    delimiter = default_delimiter
                elif n_fallbacks > 0:
                    delimiter = ","

        return spark.read.options(
            header=True,
            inferSchema=True,
            multiLine=True,
            escape="\\",
            quote=kwargs.get("quote", ""),  # quote="\"",
            delimiter=delimiter,
            encoding=encoding,
        ).csv(source)

    def _read_txt(self, **kwargs):
        return self._read_csv(**kwargs)

    def _read_xlsx(self, **kwargs):
        return self._read_excel(**kwargs)

    def _read_xlsb(self, **kwargs):
        return self._read_excel(**kwargs)

    @staticmethod
    def _read_excel(source: str, spark: SparkSession, data_address: str | list[str] = "0!", **kwargs) -> SparkDF:
        """
        Reads data from Excel files, including XLSX and XLSB formats.

        Args:
            source (str): Path to the Excel file.
            spark (SparkSession): Active Spark session.
            data_address (str | list[str]): Range or sheet reference within the Excel file (e.g., "Sheet1!A1:D10").
            kwargs: Additional configurations for reading the Excel file.

        Returns:
            SparkDF: Data from the Excel file as a Spark DataFrame.
        """
        def __read_excel(_source: str, _data_address: str, **_kwargs):
            with xlsb_to_xlsx(file_name=_source, data_address=_data_address) as source:
                # search for jars version match pyspark in https://central.sonatype.com/artifact/com.crealytics/spark-excel_2.12/3.3.2_0.19.0/versions
                # example dataAddress="0!B3:C35"
                n_rows_to_infer_schema = _kwargs.get("infer_sample_size", 500)
                return (spark.read
                        .format("excel")
                        .options(header=True,
                                 dataAddress=_data_address,  # {header_row}:{header_row+nrows+1}
                                 treatEmptyValuesAsNulls=True,
                                 usePlainNumberFormat=True,
                                 setErrorCellsToFallbackValues=True,
                                 inferSchema=True,
                                 excerptSize=n_rows_to_infer_schema,
                                 maxRowsInMemory=10000,
                                 maxByteArraySize=2147483647,
                                 tempFileThreshold=10000000,
                                 workbookPassword=None,
                                 )
                        .load(source))

        if isinstance(data_address, str):
            return __read_excel(_source=source, _data_address=data_address, **kwargs)

        elif isinstance(data_address, list):
            from cores.utils.excels import validate_data_address
            list_dfs = []
            for m in data_address:
                if check := validate_data_address(file_name=source, data_address=m):
                    df_temp = __read_excel(_source=source, _data_address=m, **kwargs)
                    print(f"""[_read_excel][data_address={m}] sheet is exist={check} row={df_temp.na.drop(how="all").count()}""")
                    list_dfs.append(df_temp)
            return df_union_by_name(list_dfs)

    @staticmethod
    def _read_parquet(source: str, spark: SparkSession, **kwargs) -> SparkDF:
        """
        Reads data from Parquet files.

        Args:
            source (str): Path to the Parquet files.
            spark (SparkSession): Active Spark session.
            kwargs: Additional configurations.

        Returns:
            SparkDF: Data from Parquet files as a Spark DataFrame.
        """
        return spark.read.parquet(source)

    def _read_mssql(self, source: str, spark: SparkSession, conn, **kwargs) -> SparkDF:
        """
        Reads data from an MSSQL database.

        Args:
            source (str): SQL query or table name.
            spark (SparkSession): Active Spark session.
            conn: MSSQL connection object.
            kwargs: Additional configurations.

        Returns:
            SparkDF: Data from MSSQL as a Spark DataFrame.
        """
        url = self.jdbc_connection_string(conn=conn, hook=self.hook_source)
        if "SELECT " in source.upper() or "SELECT\n" in source.upper(): source = f"""({source}) AS TEMP"""

        print(f"[_read_mssql] {conn.host=}")
        print(f"[_read_mssql] sql:\n", source)
        df = (
            spark.read
            .format("com.microsoft.sqlserver.jdbc.spark")
            .options(
                dbtable=source,
                url=url,
                user=conn.login,
                password=conn.password,
                Driver="com.microsoft.sqlserver.jdbc.SQLServerDriver",
                numPartitions=kwargs.get("executor_cores", 4),
                fetchsize=200000,
                trustServerCertificate=True,
                mssqlIsolationLevel="READ_COMMITTED",  # READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, or SERIALIZABLE
            )
            .load()
        )

        return df

    @staticmethod
    def _read_pandas(source: DataFrame | Callable, spark: SparkSession, **kwargs):
        """
        Converts a Pandas DataFrame to a Spark DataFrame.

        Args:
            source (DataFrame | Callable): Pandas DataFrame or a callable returning a Pandas DataFrame.
            spark (SparkSession): Active Spark session.
            kwargs: Additional configurations.

        Returns:
            SparkDF: Data as a Spark DataFrame.
        """
        if not isinstance(source, DataFrame):
            source = source()
        return spark.createDataFrame(data=source)


"""
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
op_df.write.format(op_fl_frmt).mode(operation_mode).option("partitionOverwriteMode", "dynamic").insertInto(op_dbname+'.'+op_tblname,overwrite=True)
When i tried to use the saveAsTable no matter what i do it is always wiping off all the values. 
And setting only the flag 'spark.sql.sources.partitionOverwriteMode' to dynamic doesnt seem to work. 
Hence using insertInto along with an overwrite flag inside that to achieve the desired output.
"""