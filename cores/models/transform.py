from cores.models.logs import LogMixin
from abc import abstractmethod
from pyspark.sql import DataFrame


class BaseTransform(LogMixin):
    """
    Base class for a transformation pipeline, containing information about source and destination.

    Args:
        source_path (str): Path to the source data.
        source_format (str): Format of the source data (e.g., "csv", "parquet").
        sink_path (str, optional): Path to the sink (destination) data. Defaults to None.
        sink_format (str, optional): Format of the sink data (e.g., "csv", "parquet"). Defaults to None.
    """

    def __init__(self, source_path: str, source_format: str,  sink_path: str = None, sink_format: str = None):
        super().__init__()
        self.source_format = source_format
        self.sink_format = sink_format
        self.source_path = source_path
        self.sink_path = sink_path


class BaseEngine(BaseTransform):
    """
    Base class for a data processing engine, requiring implementation of reading, transforming, and writing methods.

    Abstract Methods:
        read_source(**kwargs): Read data from the source.
        write_sink(**kwargs): Write data to the sink.
        transform(**kwargs): Perform the transformation logic.
    """

    @abstractmethod
    def read_source(self, **kwargs):
        """
        Read data from the source.

        Args:
            **kwargs: Additional keyword arguments for reading data.

        Raises:
            NotImplementedError: If not implemented in a derived class.
        """
        raise NotImplementedError()

    @abstractmethod
    def write_sink(self, **kwargs):
        """
        Write data to the sink.

        Args:
            **kwargs: Additional keyword arguments for writing data.

        Raises:
            NotImplementedError: If not implemented in a derived class.
        """
        raise NotImplementedError()

    @abstractmethod
    def transform(self, **kwargs):
        """
        Perform the transformation logic.

        Args:
            **kwargs: Additional keyword arguments for transforming data.

        Raises:
            NotImplementedError: If not implemented in a derived class.
        """
        raise NotImplementedError()


class PipeTransform(LogMixin):
    """
    Base class for a pipeline in a PySpark engine, processing data via the `__call__` method.

    Args:
        *args: Positional arguments for the pipeline.
        **kwargs: Keyword arguments for the pipeline.

    Methods:
        __call__(df: DataFrame, **kwargs): Process the given DataFrame. Must be implemented in a derived class.
    """

    def __init__(self, *args, **kwargs):
        super().__init__()
        self.args = args
        self.kwargs = kwargs

    def __call__(self, df: DataFrame, **kwargs):
        """
        Process the given DataFrame.

        Args:
            df (DataFrame): Input DataFrame to process.
            **kwargs: Additional keyword arguments.

        Raises:
            NotImplementedError: If not implemented in a derived class.
        """
        raise NotImplementedError(f"[{self.__class__}] Need to implement this method __call__")
