from pyspark.sql.functions import *
from pyspark.sql import SparkSession

from cores.models.transform import PipeTransform
from tempfile import TemporaryDirectory
from contextlib import contextmanager
from pathlib import Path


class CTXHandleCacheDataFromDB(PipeTransform):
    """
    A context manager for caching data from a database into a temporary storage directory.

    Inherits:
        - `PipeTransform`: Base class for defining transformations in a pipeline.

    Attributes:
        temp_path (str): Default base path for temporary storage. Adjusts dynamically based on availability.

    Methods:
        __call__(self, df: DataFrame, metadata=None, spark: SparkSession=None, **kwargs):
            Handles caching of a DataFrame to a temporary directory and reloads it as a new DataFrame.

            Args:
                df (DataFrame): Input DataFrame to cache.
                metadata: Metadata object containing sink configuration details.
                spark (SparkSession): The active Spark session.
                **kwargs: Additional arguments for customization.
                    - executor_cores (int): Number of executor cores to use for repartitioning. Default is 4.

            Yields:
                DataFrame: Reloaded DataFrame from the temporary parquet storage.

    Example:
        with CTXHandleCacheDataFromDB()(df, metadata=metadata, spark=spark) as cached_df:
            # Use `cached_df` as needed
            cached_df.show()
    """
    temp_path = "/mnt/d/EDW-PROJECT-TEMP/"

    @contextmanager
    def __call__(self, df: DataFrame, metadata = None, spark: SparkSession = None, **kwargs):
        """
        Caches the input DataFrame into a temporary directory and reloads it as a new DataFrame.

        Args:
            df (DataFrame): Input DataFrame to cache.
            metadata: Metadata object containing sink configuration details.
            spark (SparkSession): The active Spark session.
            **kwargs: Additional arguments for customization.
                - executor_cores (int): Number of executor cores to use for repartitioning. Default is 4.

        Yields:
            DataFrame: Reloaded DataFrame from the temporary parquet storage.

        Process:
            - If the sink format is not supported (e.g., not "mssql"), skips caching and yields the original DataFrame.
            - Writes the DataFrame as parquet files to a temporary directory.
            - Reads the parquet files back into a new DataFrame and yields it.
            - Logs progress and row counts.

        Example:
            with CTXHandleCacheDataFromDB()(df, metadata=metadata, spark=spark) as cached_df:
                print(f"Cached row count: {cached_df.count()}")
        """
        if metadata.sink_format not in ["mssql", ]:
            yield df
            return

        executor_cores = kwargs.get("executor_cores", 4)

        # Handle temp path
        prefix_name = metadata.sink_path.replace(".", "_") + "_"
        temp_path = self.temp_path if Path(self.temp_path).exists() else None

        with TemporaryDirectory(dir=temp_path, prefix=prefix_name) as target_dir:
            print(f"[CTXHandleCacheDataFromDB] start write parquet to {target_dir}")
            df.repartition(executor_cores).write.mode("overwrite").parquet(target_dir)
            print(f"[CTXHandleCacheDataFromDB] finish write")
            df_new = spark.read.parquet(target_dir)
            print(f"[CTXHandleCacheDataFromDB] total rows={df_new.count()}")
            yield df_new


if __name__ == "__main__":
    from cores.hooks.sparks import SparkHook
    from cores.engines.spark_engine import SparkEngine

    list_jars = [
        "com.microsoft.azure:spark-mssql-connector_2.12:1.3.0-BETA",
        "com.microsoft.sqlserver:mssql-jdbc:12.6.1.jre8"
    ]  #
    # list_jars += ["com.crealytics:spark-excel_2.12:3.3.2_0.19.0"] if self.source_format.lower() in ["excel", "xlsb", "xlsx"] else []
    list_jars += ["com.crealytics:spark-excel_2.12:3.3.3_0.20.2"] if "" in ["excel", "xlsb", "xlsx"] else []

    hook = SparkHook(jars=list_jars)
    hook.spark_config.set_spark_memory("4G")
    hook.spark_config.set_master("local[2]")

    fake_engine = SparkEngine(
        source_path="/mnt/e/EDW-BACKUP-TEMP/sFTP_INVOICE_DTL/2024-05-01/",
        source_format="parquet",
        sink_path="",
        sink_format="mssql",
    )

    with SparkHook().spark as spark:
        df_temp = spark.read.parquet("/mnt/e/EDW-BACKUP-TEMP/sFTP_INVOICE_DTL/2024-05-01")

        with CTXHandleCacheDataFromDB()(df=df_temp, metadata=fake_engine, spark=spark) as output:
            output.show(10)

