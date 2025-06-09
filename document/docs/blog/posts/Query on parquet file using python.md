---
pin: false
date: 2025-01-23
categories:
  - Data transformation
  - FAQ
---

## How to query data directly from parquet file ?

``` py linenums="1"
import polars as pl
from datetime import datetime

# read parquet file using polars
df = pl.scan_parquet(["/mnt/e/EDW-BACKUP-TEMP/RAW_DATA_SECONDARY/2018-??-??/*.parquet"], low_memory=True, glob=True)

# filter data on column SALES_BY_MONTH between datetime(2018, 1, 1) and datetime(2019, 12, 31)
df = df.filter(pl.col("SALES_BY_MONTH").is_between(datetime(2018, 1, 1), datetime(2019, 12, 31)))

# aggregate data
df_agg = (
    df
    .group_by(["SITECODE", "ROUTECODE", "SALESMANCODE", "SALES_BY_MONTH", "OFFDATE"])
    .agg(
        NOPROMONOVAT=pl.sum("NOPROMONOVAT"), 
        WITHPROMONOVAT=pl.sum("WITHPROMONOVAT"),
    )
)

with pl.Config(tbl_cols=-1):
    print(df_agg)
```
