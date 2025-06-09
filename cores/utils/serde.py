from pandas import DataFrame
from cores.utils.providers import ProvideBenchmark
from polars import DataFrame as PDataFrame, LazyFrame
from dask.dataframe import DataFrame as DDataFrame


def f_base64(msg: str, method: str = "encode", encode_utf: str = "utf-8"):
    import base64

    if method == "encode":
        result = base64.b64encode(msg.encode(encode_utf))
    else:
        result = base64.b64decode(msg.encode(encode_utf))

    return result.decode(encode_utf)


# def serialize(data):
#     # import airflow.serialization.serializers.pandas
#
#     if not isinstance(data, (PDataFrame, LazyFrame, DDataFrame)):
#         return data
#
#     from cloudpickle import dumps
#     from blosc import compress
#
#     output = DataFrame([{
#         "_framework_container_": compress(dumps(data)).hex()
#     }])
#
#     print("dataframe container size:", output.memory_usage(deep=True).sum() / 1024, "KBs")
#
#     return output
#
#
# def deserialize(dataframe: DataFrame):
#     from cloudpickle import loads
#     from blosc import decompress
#
#     columns = dataframe.columns.to_list()
#
#     match columns:
#         case ["_framework_container_"]:
#
#             rebuild_object = loads(decompress(bytes.fromhex(dataframe["_framework_container_"].values[0])))
#             return rebuild_object
#         case _:
#             return dataframe


def new_serialize(dataframe: DataFrame):
    import pyarrow as pa
    from pyarrow import parquet as pq
    # from blosc import compress

    table = pa.Table.from_pandas(dataframe)
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue().hex().decode("utf-8")


def new_deserialize(data):
    from pyarrow import parquet as pq
    from io import BytesIO
    # from blosc import decompress

    with BytesIO(bytes.fromhex(data)) as buf:
        df = pq.read_table(buf).to_pandas()
    return df
