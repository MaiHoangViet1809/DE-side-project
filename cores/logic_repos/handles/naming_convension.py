from pyspark.sql.functions import *

from cores.models.transform import PipeTransform
from cores.logic_repos.column_naming_rule import p_column_name_enforce


class HandleColumnNamingConvention(PipeTransform):
    """
    A transformation class to enforce column naming conventions on a DataFrame.

    Inherits:
        - `PipeTransform`: Base class for transformations in a pipeline.

    Methods:
        __call__(self, df: DataFrame, *args, **kwargs):
            Applies a column naming convention to the DataFrame.
    """
    def __call__(self, df: DataFrame, *args, **kwargs):
        """
        Applies column naming conventions to a DataFrame by renaming columns.

        Args:
            df (DataFrame): The input DataFrame whose columns need to be renamed.
            *args: Additional positional arguments (not used).
            **kwargs: Additional keyword arguments (not used).

        Returns:
            DataFrame: The DataFrame with columns renamed according to the naming convention.

        Process:
            - Enforces naming rules using the `p_column_name_enforce` function.
            - Renames all columns in the DataFrame based on the enforced rules.

        Example:
            >>> df = spark.createDataFrame([(1, "value")], ["Column 1", "Value Column"])
            >>> transformer = HandleColumnNamingConvention()
            >>> transformed_df = transformer(df)
            >>> transformed_df.columns
            ['COLUMN_1', 'VALUE_COLUMN']
        """
        list_col_name = [p_column_name_enforce(m) for m in df.columns]
        df = df.toDF(*list_col_name)
        return df
