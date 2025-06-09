from cores.engines.odbc_engine import ODBCEngine as _ODBCEngine


class DaskEngine(_ODBCEngine):
    from dask.dataframe import DataFrame as DaskDataFrame

    def _read_csv(self, delimiter: str = ",", **kwargs) -> DaskDataFrame:
        from dask.dataframe import read_csv
        try:
            df = read_csv(self.source_path, encoding='utf-16', delimiter=delimiter, low_memory=True, dtype="string", **kwargs)
        except Exception as e:
            if "'utf-16' codec can't decode byte" in str(e):
                df = read_csv(self.source_path, encoding='utf-8', delimiter=delimiter, low_memory=True, dtype="string", **kwargs)
            else:
                raise e
        return df

    def _read_parquet(self, **kwargs) -> DaskDataFrame:
        from dask.dataframe import read_parquet
        return read_parquet(self.source_path, engine="pyarrow", **kwargs)

    def _read_excel(self, sample_size: int = 50, **kwargs) -> DaskDataFrame:
        from cores.utils.excels import read_excel
        from dask.dataframe import from_pandas
        return from_pandas(read_excel(source_path=self.source_path, sample_size=sample_size, **kwargs), npartitions=4)

    def _read_sql(self, hook) -> DaskDataFrame:
        from dask.dataframe import from_pandas
        return from_pandas(hook.get_pandas_df(self.source_path), npartitions=4)

    def write_sink(self, df: DaskDataFrame, hook, **kwargs):
        match self.sink_format:
            case "mssql":
                try:
                    hook.insert_many(dst_table=self.sink_path, data=df.compute().to_records(index=False).tolist())
                except Exception as E:
                    import traceback
                    traceback.print_exc()
                    raise E
            case "parquet":
                df.compute().to_parquet(path=self.sink_path, index=False)
            case _ :
                raise ValueError(f"[{self.__class__.__name__}] sink as {self.sink_format} is not supported")

