from dataclasses import dataclass

from cores.utils.shells import run_sh
from cores.utils.env_info import is_in_docker, has_java_home, set_java_home
# from cores.utils.configs import FrameworkConfigs


@dataclass
class SparkConfig:
    """
    Configuration class for Spark settings.

    Attributes:
        spark_master (str): The Spark master URL (e.g., "local[2]").
        spark_local_dir (str): Directory for Spark temporary files.
        spark_eventLog_enabled (str): Whether event logging is enabled. Default is "false".
        spark_ui_showConsoleProgress (str): Whether to show console progress. Default is "true".
        spark_sql_parquet_int96RebaseModeInRead (str): Parquet INT96 rebase mode for reading. Default is "CORRECTED".
        spark_sql_parquet_int96RebaseModeInWrite (str): Parquet INT96 rebase mode for writing. Default is "CORRECTED".
        spark_sql_parquet_datetimeRebaseModeInRead (str): Parquet datetime rebase mode for reading. Default is "CORRECTED".
        spark_sql_parquet_datetimeRebaseModeInWrite (str): Parquet datetime rebase mode for writing. Default is "CORRECTED".
        spark_sql_adaptive_enabled (str): Whether adaptive query execution is enabled. Default is "true".
        spark_cores_max (int): Maximum number of Spark cores. Default is 4.
        spark_executor_cores (int): Number of cores per executor. Default is 2.
        spark_executor_memory (str): Memory allocation for executors. Default is "2G".
        spark_driver_cores (int): Number of cores for the driver. Default is 2.
        spark_driver_maxResultSize (str): Maximum size for driver results. Default is "6G".
        spark_driver_memory (str): Memory allocation for the driver. Default is "2G".
        spark_speculation (str): Whether speculative execution is enabled. Default is "true".

    Methods:
        config_conversion(value: str, d1: str = ".", d2: str = "_") -> str:
            Converts configuration keys between dot and underscore formats.

        __getitem__(key: str):
            Retrieves the value for a given configuration key.

        __setitem__(key: str, value):
            Sets the value for a given configuration key.

        keys():
            Returns a list of configuration keys.

        get_all_configs():
            Returns all configuration settings as a dictionary.

        set_spark_memory(value: str = "2G"):
            Sets memory allocation for both driver and executors.

        set_master(value: str = "local[2]"):
            Sets the Spark master.

        set_hive_uri(uri: str):
            Sets the Hive metastore URI.

        set_jars_package(list_packages: list):
            Sets the list of Spark JARs packages.

        set_app_name(name: str):
            Sets the Spark application name.

        set_driver_com(driver_host: str, driver_bind_address: str, driver_port: str | int, block_manager_port: str | int, ui_port: str | int):
            Configures driver communication settings.

        set_cluster_mode(master: str, value: bool = False, port_start: int = 30000):
            Configures the cluster mode and related driver settings.
    """
    spark_master = None

    spark_local_dir = "/mnt/e/Airflow-Temp/spark-tmp"  # "/tmp/spark-temp"
    spark_eventLog_enabled = "false"
    spark_ui_showConsoleProgress = "true"

    spark_sql_parquet_int96RebaseModeInRead = "CORRECTED"
    spark_sql_parquet_int96RebaseModeInWrite = "CORRECTED"
    spark_sql_parquet_datetimeRebaseModeInRead = "CORRECTED"
    spark_sql_parquet_datetimeRebaseModeInWrite = "CORRECTED"

    # spark_hadoop_fs_s3a_access_key = None
    # spark_hadoop_fs_s3a_secret_key = None
    # spark_hadoop_fs_s3a_impl = "org.apache.hadoop.fs.s3a.S3AFileSystem"
    # spark_hadoop_fs_s3a_aws_credentials_provider = "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"

    # spark_dynamicAllocation_enabled = "false"
    # spark_dynamicAllocation_shuffleTracking_enabled = "true"
    # spark_dynamicAllocation_minExecutors = 0
    # spark_dynamicAllocation_executorIdleTimeout = 60
    # spark_dynamicAllocation_cachedExecutorIdleTimeout = 60
    # spark_dynamicAllocation_shuffleTracking_timeout = 60
    # spark_sql_shuffle_partitions = 128
    spark_sql_adaptive_enabled = "true"
    # spark_scheduler_mode = "FAIR"
    # spark_scheduler_pool = "production"
    spark_cores_max = 4
    spark_executor_cores = 2
    spark_executor_memory = "2G"

    spark_driver_cores = 2
    spark_driver_maxResultSize = "6G"
    spark_driver_memory = "2G"  # "29G"

    # spark_hadoop_fs_s3a_fast_upload = "true"
    spark_speculation = "true"

    # "spark_hadoop_fs_s3a_bucket_all_committer_magic_enabled = "true",
    # "spark_driver_rpc_io_clientThreads = 4,
    # "spark_driver_rpc_netty_dispatcher_numThreads = 4,
    # "spark_memory_offHeap_enabled = "true",
    # "spark_memory_offHeap_size = "4G",

    @staticmethod
    def config_conversion(value, d1: str = ".", d2: str = "_"):
        return value.replace(d1, d2)

    def __getitem__(self, key: str):
        adj_name = self.config_conversion(key)
        if adj_name in self.__dict__:
            return getattr(self, adj_name)
        else:
            raise IndexError(f"{key} is not found in SparkConfig")

    def __setitem__(self, key: str, value):
        adj_name = self.config_conversion(key)
        return setattr(self, adj_name, value)

    def keys(self):
        return list(self.config_conversion(k, "_", ".") for k in self.__dict__.keys())

    def get_all_configs(self):
        instance_config = {self.config_conversion(k, "_", "."): v for k, v in self.__dict__.items()}
        default_config = {self.config_conversion(k, "_", "."): v for k, v in self.__class__.__dict__.items() if str(k).startswith("spark_")}
        return {**default_config, **instance_config}

    def set_spark_memory(self, value: str = "2G"):
        self.spark_driver_memory = value
        self.spark_executor_memory = value

    def set_master(self, value: str = "local[2]"):
        self.spark_master = value

    def set_hive_uri(self, uri: str):
        self["hive_metastore_uris"] = uri

    def set_jars_package(self, list_packages: list):
        # defaultPackages = FrameworkConfigs.Hooks.Spark.SPARK_PACKAGES
        # self["spark.jars.packages"] = ",".join(list(set(list_packages + defaultPackages)))
        self["spark.jars.packages"] = ",".join(list(set(list_packages)))

    def set_app_name(self, name: str):
        self["spark.app.name"] = name

    def set_driver_com(self, driver_host: str, driver_bind_address: str, driver_port: str | int, block_manager_port: str | int, ui_port: str | int):
        self["spark.driver.host"] = driver_host  # IP of host which driver run in
        self["spark.driver.bindAddress"] = driver_bind_address
        self["spark.driver.port"] = driver_port
        self["spark.driver.blockManager.port"] = block_manager_port
        self["spark.ui.port"] = ui_port

    def set_cluster_mode(self, master: str, value=False, port_start: int = 30000):
        if not has_java_home():
            set_java_home()

        if value:
            self.set_master(master)
            # driver host in this case will point to dev env internal ip
            self.set_driver_com(driver_host="localhost",
                                driver_bind_address="localhost",
                                driver_port=port_start,
                                block_manager_port=port_start + 40,
                                ui_port=port_start + 4040, )

        elif is_in_docker():  # case run in docker with local resource
            _, output = run_sh("""cat /etc/hosts | grep "$HOSTNAME" | awk -F" " '{print $1}'""")
            self.set_driver_com(driver_host="localhost",
                                driver_bind_address=output[0],
                                driver_port=port_start,
                                block_manager_port=port_start + 40,
                                ui_port=port_start + 4040, )

        else:  # case run in local resource
            self.set_master(master)
            self.set_spark_memory("2G")


class SparkHook:
    """
    A utility class to manage Spark sessions and configurations.

    Attributes:
        spark_config (SparkConfig): The configuration object for Spark.

    Methods:
        __init__(**kwargs):
            Initializes the SparkHook with custom configurations.

        _build_spark():
            Builds and configures a Spark session.

        spark:
            Returns the active Spark session.

        get_list_configs():
            Retrieves a list of all available configuration keys.
    """
    def __init__(self, **kwargs):
        """
        Initializes the SparkHook instance.

        Args:
            **kwargs: Custom configuration parameters for the Spark session, including:
                - master (str): The Spark master URL. Default is "local[2]".
                - cluster_mode (bool): Whether to enable cluster mode. Default is False.
                - jars (list): List of JARs to include.
                - app_name (str): Name of the Spark application. Default is "pyspark".
        """
        self.spark_config = SparkConfig()
        self.spark_config.set_cluster_mode(master=kwargs.get("master", "local[2]"),
                                           value=kwargs.pop("cluster_mode", False))
        self.spark_config.set_jars_package(list_packages=kwargs.get("jars", []))
        self.spark_config.set_app_name(kwargs.get("app_name", getattr(self, "table_name", "pyspark")))

    def _build_spark(self):
        """
        Builds and configures a Spark session.

        Returns:
            SparkSession: A configured Spark session object.
        """
        from pyspark.sql import SparkSession
        spark_builder = SparkSession.builder
        for k, v in self.spark_config.get_all_configs().items():
            spark_builder = spark_builder.config(k, v)

        if "hive.metastore.uris" in self.spark_config.keys():
            spark_builder = spark_builder.enableHiveSupport()

        return spark_builder

    @property
    def spark(self):
        """
        Provides access to the active Spark session.

        Returns:
            SparkSession: The active Spark session.
        """
        return self._build_spark().getOrCreate()

    def get_list_configs(self):
        """
        Retrieves a list of all configuration keys.

        Returns:
            list[str]: List of configuration keys.
        """
        return [self.spark_config.config_conversion(m) for m in list(self.spark_config.get_all_configs().keys())]


if __name__ == "__main__":
    # unit test
    spark_config = SparkConfig()
    spark_config.set_cluster_mode(master="local[4]", value=False)
    print(spark_config.get_all_configs())

    hook = SparkHook()
    print(hook.get_list_configs())
