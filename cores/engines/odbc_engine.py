from pandas import DataFrame

from cores.models.transform import BaseEngine
from cores.logic_repos.column_naming_rule import p_column_name_enforce
from cores.utils.configs import FrameworkConfigs


def handle_na(df: DataFrame) -> DataFrame:
    from numpy import nan
    from pandas import NA, NaT
    return (
        df
        .replace({nan: None, NA: None, NaT: None, "": None})
        # .replace(r'\r+|\n+', ' ', regex=True)
    )


class ODBCEngine(BaseEngine):
    def read_source(self, hook=None, **kwargs):
        match self.source_format:
            case "txt":
                df = self._read_csv()
            case "csv":
                df = self._read_csv(delimiter=",")
            case "parquet":
                df = self._read_parquet()
            case "excel" | "xlsx":
                df = self._read_excel()
            case "sql" | "mssql":
                df = self._read_sql(hook)
            case "dataframe":
                df = self.source_path
            case _:
                raise ValueError(self.source_format + " is not supported")

        df = handle_na(df)
        df = df.rename(columns=p_column_name_enforce)
        # if df.shape[0] == 0:
        #     from airflow.exceptions import AirflowFailException
        #     raise AirflowFailException("dataframe has 0 rows - stop job")
        return df

    def write_sink(self, df: DataFrame, hook, **kwargs):
        match self.sink_format:
            case "mssql" | "sql":
                try:
                    # hook.insert_many(dst_table=self.sink_path, data=df.to_records(index=False).tolist())

                    batch_size = FrameworkConfigs.Ingestion.BATCH_SIZE
                    data = [m for m in df.to_records(index=False).tolist()]

                    def batch(iterable, n=1):
                        l = len(iterable)
                        for ndx in range(0, l, n):
                            yield iterable[ndx:min(ndx + n, l)]

                    total_rows = 0
                    for i, b in enumerate(batch(data, batch_size)):
                        total_rows += len(b)
                        print("current batch:", i, total_rows)
                        hook.insert_many(dst_table=self.sink_path, data=list(b))

                except Exception as E:
                    import traceback
                    traceback.print_exc()
                    raise E
            case "parquet":
                df.to_parquet(path=self.sink_path, index=False)

            case "dataframe":
                print(f"[write_sink] total rows: {df.size}")
                return df
            case _ :
                raise ValueError(f"[{self.__class__.__name__}] sink as {self.sink_format} is not supported")

    def transform(self, hook, **kwargs):
        df = self.read_source(hook=hook)
        return self.write_sink(df, hook=hook, **kwargs)

    def _read_csv(self, delimiter: str = FrameworkConfigs.Ingestion.TEXT_DELIMITER, **kwargs) -> DataFrame:
        from pandas import read_csv
        from cores.utils.encoding import predict_encoding
        encoding = predict_encoding(self.source_path)
        print("check encoding", encoding)
        return read_csv(self.source_path, encoding=encoding, delimiter=delimiter, low_memory=True, dtype="string", **kwargs)

    def _read_parquet(self, **kwargs) -> DataFrame:
        from pandas import read_parquet
        return read_parquet(self.source_path, engine="pyarrow", **kwargs)

    def _read_excel(self, sample_size: int = 50, **kwargs) -> DataFrame:
        from cores.utils.excels import read_excel
        return read_excel(source_path=self.source_path, sample_size=sample_size, **kwargs)

    def _read_sql(self, hook) -> DataFrame:
        from pandas import read_sql
        from contextlib import closing

        with closing(hook.get_conn()) as conn:
            df = read_sql(self.source_path, con=conn)
        return df
