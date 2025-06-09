from pyspark.sql.functions import *
from cores.models.transform import PipeTransform
from datetime import datetime


class HandleDate(PipeTransform):
    """
    A transformation class to handle and standardize date columns in a DataFrame.

    Inherits:
        - `PipeTransform`: Base class for transformations in a pipeline.

    Attributes:
        column_names (list[str]): List of column names to process. If None, automatically detects date-like columns.

    Methods:
        __call__(self, df: DataFrame, **kwargs):
            Processes the specified columns by standardizing date representations.

    Example:
        >>> transformer = HandleDate(column_names=["event_date", "created_at"])
        >>> transformed_df = transformer(df)
    """
    def __init__(self, column_names: list[str] = None, **kwargs):
        """
        Initializes the HandleDate transformation.

        Args:
            column_names (list[str], optional): List of columns to process. Defaults to None for auto-detection.
            **kwargs: Additional arguments for the base class.
        """
        super().__init__(**kwargs)
        self.column_names = column_names

    def __call__(self, df: DataFrame, **kwargs):
        """
        Processes the DataFrame by converting date-like columns to standardized date formats.

        Args:
            df (DataFrame): The input DataFrame.
            **kwargs: Additional arguments for customization.

        Returns:
            DataFrame: The transformed DataFrame with standardized date columns.

        Process:
            - Detects date-like columns based on their data types or naming patterns if `column_names` is not provided.
            - Converts `timestamp` columns to `date`.
            - Converts integer-based date representations to `date` using a default base date ("1899-12-30").

        Example:
            >>> transformer = HandleDate()
            >>> transformed_df = transformer(df)
        """
        dtypes = dict(df.dtypes)
        DEFAULT_GEO_DT_MS = "1899-12-30"
        if not self.column_names:
            self.column_names = [m for m in df.columns
                                 if dtypes[m].lower() in ["timestamp", "datetime", "date"]
                                 or (dtypes[m].lower() == "int"
                                     and (m.lower().endswith("_dt")
                                          or "_dttm" in m.lower()
                                          or "date" in m.lower()
                                          )
                                     )]

        for column_name in self.column_names:
            column_type = dtypes[column_name]
            match column_type.lower():
                case "timestamp":
                    print(f"[HandleDatetime] {column_name}--{column_type=} match timestamp")
                    df = df.withColumn(column_name, col(column_name).cast("date"))
                    # df = df.withColumn(column_name, col(column_name).cast("timestamp"))
                case "int":
                    print(f"[HandleDatetime] {column_name}--{column_type=} match int")
                    df = df.withColumn(column_name, date_add(to_date(lit(DEFAULT_GEO_DT_MS), "yyyy-MM-dd"), col(column_name)))
                    # df = df.withColumn(column_name, to_timestamp(date_add(to_date(lit(DEFAULT_GEO_DT_MS), "yyyy-MM-dd"), col(column_name))))
                case _:
                    print(f"[HandleDatetime] {column_name}--{column_type=} - do not support this type")
        return df


class HandleDatetimePattern(PipeTransform):
    """
    A transformation class to parse and convert date or datetime columns in a DataFrame using a specified pattern.

    Inherits:
        - `PipeTransform`: Base class for transformations in a pipeline.

    Attributes:
        column_names (list[str]): List of column names to process.
        input_pattern (str): Input datetime pattern to parse the columns.

    Methods:
        - __call__(self, df: DataFrame, **kwargs): Parses columns using the specified pattern.
        - udf_parse_datetime(data, pattern, *args): Static method for parsing datetime values.

    Example:
        >>> transformer = HandleDatetimePattern(column_names=["event_time"], input_pattern="%Y-%m-%d %H:%M:%S")
        >>> transformed_df = transformer(df)
    """

    def __init__(self, column_names: list[str], input_pattern: str, **kwargs):
        """
        Initializes the HandleDatetimePattern transformation.

        Args:
            column_names (list[str]): List of columns to process.
            input_pattern (str): Pattern to parse datetime values.
            **kwargs: Additional arguments for the base class.
        """
        super().__init__(**kwargs)
        self.column_names = column_names
        self.input_pattern = input_pattern

    def __call__(self, df: DataFrame, **kwargs):
        """
        Parses specified columns in the DataFrame using the input pattern.

        Args:
            df (DataFrame): The input DataFrame.
            **kwargs: Additional arguments for customization.

        Returns:
            DataFrame: The transformed DataFrame with parsed datetime columns.

        Process:
            - Applies `udf_parse_datetime` to each specified column if its type is `string` or `int`.
            - Converts the parsed values to `timestamp`.

        Example:
            >>> transformer = HandleDatetimePattern(column_names=["date_column"], input_pattern="%Y-%m-%d")
            >>> transformed_df = transformer(df)
        """
        dtypes = dict(df.dtypes)
        list_columns =  df.columns

        for column_name in self.column_names:
            column_type = dtypes[column_name]
            print("[HandleDatetimePattern]", column_name, column_type)
            if column_type in ("string", "int"):
                df = df.withColumn(column_name, udf(self.udf_parse_datetime)(col(column_name), lit(self.input_pattern), *[col(m) for m in list_columns]).cast("timestamp"))
        return df

    @staticmethod
    def udf_parse_datetime(data, pattern, *args):
        """
        Parses datetime values using the specified pattern or Excel-style ordinal numbers.

        Args:
            data: The value to parse.
            pattern (str): The datetime pattern to use for parsing.
            *args: Additional arguments for debugging.

        Returns:
            str: The parsed date string in "YYYY-MM-DD" format.

        Process:
            - Checks if the value is a number and interprets it as an ordinal date (e.g., Excel-style).
            - Parses string-based datetime values using the given pattern.
            - Raises an exception for invalid values.

        Example:
            >>> HandleDatetimePattern.udf_parse_datetime("2025-01-01", "%Y-%m-%d")
            '2025-01-01'
        """
        def is_number_try_except(s):
            """ Returns True if string is a number. """
            try:
                float(s)
                return True
            except ValueError:
                return False

        if data:
            try:
                if is_number_try_except(data):
                    dt = datetime.fromordinal(datetime(1900, 1, 1).toordinal() + int(float(data)) - 2)
                    return dt.strftime("%Y-%m-%d")
                return datetime.strptime(str(data), pattern).strftime("%Y-%m-%d")
            except Exception as e:
                print("Error on rows:", *args)
                raise e


if __name__ == "__main__":
    from cores.hooks.sparks import SparkHook

    spark_hook = SparkHook(jars=[])
    spark_hook.spark_config.set_spark_memory("6G")
    spark_hook.spark_config.set_master("local[4]")

    with spark_hook.spark as spark:
        df_temp = spark.createDataFrame(
            [
                ('M12.2023',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',),
                ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',), ('M1.2024',),
            ],
            ["MONTH"])

        # approach 1: native pyspark function with complex handle pattern
        start_time = datetime.now()
        df_2 = df_temp.withColumns(dict(
            MONTH=to_timestamp(lpad(regexp_replace(col("MONTH"), 'M', ''), 7, '0'), "MM.yyyy")
        ))

        df_2.show(10)
        print("processed time=", (datetime.now() - start_time).total_seconds())

        # approach 2: create python udf and using datetime.strptime pattern
        start_time = datetime.now()
        df_3 = HandleDatetimePattern(column_names=["MONTH"], input_pattern="M%m.%Y")(df_temp)
        df_3.show(10, truncate=False)
        print("processed time=", (datetime.now() - start_time).total_seconds())
