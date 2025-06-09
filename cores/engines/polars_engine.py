from cores.models.transform import BaseEngine
from cores.logic_repos.column_naming_rule import p_column_name_enforce
from cores.utils.configs import FrameworkConfigs
from cores.utils.debug import print_table

from polars import DataFrame, LazyFrame
import polars as pl

TVFrame = DataFrame | LazyFrame


class PolarsEngine(BaseEngine):
    def read_source(self, hook, **kwargs):
        match self.source_format:
            case "csv" | "txt":
                df = self._read_csv()
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

        columns = df.columns
        adj_columns = {k: p_column_name_enforce(k) for k in columns}
        df = df.rename(adj_columns)
        return df

    def write_sink(self, df: TVFrame, hook, optimized_insert=True, **kwargs):
        match self.sink_format:
            case "mssql":
                try:
                    if optimized_insert:
                        self._write_mssql_pandas_to_sql(df, hook)
                    else:
                        self._write_mssql_native_polars(df, hook)
                except Exception as E:
                    import traceback
                    traceback.print_exc()
                    raise E
            case "parquet":
                df.lazy().sink_parquet(path=self.sink_path)
            case _:
                raise ValueError(f"[{self.__class__.__name__}] sink as {self.sink_format} is not supported")

    def _write_mssql_native_polars(self, df, hook):
        df.lazy().collect().write_database(table_name=self.sink_path,
                                           connection=hook.get_connection_uri(),
                                           if_table_exists="append",
                                           )

    def _write_mssql_pandas_to_sql(self, df: TVFrame, hook):
        """
        fastest method importing dataframe to mssql
        """
        hook.insert_many(dst_table=self.sink_path,
                         data=df
                         .lazy().collect()
                         .to_pandas()
                         .to_records(index=False).tolist()
                         )

    def transform(self, hook, optimized_insert=True, **kwargs):
        df = self.read_source(hook=hook)
        print_table(df.lazy().collect().head(5).to_pandas(), 5)
        self.write_sink(df, hook=hook, optimized_insert=optimized_insert, **kwargs)

    def _read_csv(self, delimiter: str = FrameworkConfigs.Ingestion.TEXT_DELIMITER, **kwargs) -> TVFrame:
        from cores.utils.encoding import predict_encoding
        encoding = predict_encoding(self.source_path)
        print("check encoding", encoding)
        with open(self.source_path, 'r', encoding=encoding) as file:
            return pl.read_csv(source=file.read().encode('utf-8'),
                               has_header=True,
                               separator=delimiter,
                               encoding='utf8',
                               low_memory=False,
                               skip_rows=0,
                               infer_schema_length=FrameworkConfigs.Ingestion.TEXT_SCHEMA_INFER_LENGTH,
                               **kwargs)

    def _read_parquet(self, **kwargs) -> TVFrame:
        return pl.scan_parquet(source=self.source_path, **kwargs)

    def _read_excel(self, sample_size: int = 100, **kwargs) -> TVFrame:

        try:
            self.msg("start read excel")
            return pl.read_excel(source=self.source_path,
                                 engine="xlsx2csv",
                                 xlsx2csv_options={"skip_hidden_rows": False},
                                 read_csv_options=dict(
                                     has_header=True,
                                     encoding='utf-8',
                                     infer_schema_length=FrameworkConfigs.Ingestion.TEXT_SCHEMA_INFER_LENGTH,
                                     batch_size=FrameworkConfigs.Ingestion.BATCH_SIZE,
                                     n_threads=2,
                                 ),
                                 **kwargs)
        except Exception as e:
            from cores.utils.excels import get_header_and_rows

            self.msg("start estimate header line number")
            header_row_index, actual_row = get_header_and_rows(excel_path=self.source_path, sample_size=sample_size)

            self.msg("start read excel")
            return pl.read_excel(source=self.source_path,
                                 engine="xlsx2csv",
                                 xlsx2csv_options={"skip_hidden_rows": False},
                                 read_csv_options=dict(
                                     has_header=True,
                                     encoding='utf-8',
                                     skip_rows=header_row_index,
                                     infer_schema_length=FrameworkConfigs.Ingestion.TEXT_SCHEMA_INFER_LENGTH,
                                     batch_size=FrameworkConfigs.Ingestion.BATCH_SIZE,
                                     n_threads=2,
                                 ),
                                 **kwargs)

    def _read_sql(self, hook) -> TVFrame:
        if " " not in self.source_path:
            sql = f"SELECT * FROM {self.source_path}"
        else:
            sql = self.source_path
        return pl.read_database_uri(query=sql, uri=hook.get_connection_uri(), engine="connectorx")
