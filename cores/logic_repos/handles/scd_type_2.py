import pandas as pd


# Define the SCD Type 2 logic function
# https://amsayed.hashnode.dev/etl-from-sql-server-to-azure-synapse-analytics-with-slowly-changing-dimensions-scd-type-2-using-7a4b00077902
def scd2_merge(current_data, new_data, key_columns, scd_columns):
    merged_data = pd.merge(
    current_data,
    new_data,
    on=key_columns,
    how="outer",
    suffixes=("_current", "_new"),
    indicator=True
    )

    new_records = merged_data[merged_data["_merge"] == "right_only"]
    unchanged_records = merged_data[merged_data["_merge"] == "both"]
    expired_records = merged_data[merged_data["_merge"] == "left_only"]

    for scd_col in scd_columns:
        changed_records = unchanged_records[unchanged_records[f"{scd_col}_current"] != unchanged_records[f"{scd_col}_new"]]
        unchanged_records = unchanged_records[unchanged_records[f"{scd_col}_current"] == unchanged_records[f"{scd_col}_new"]]

        if not changed_records.empty:
            changed_records = changed_records.drop("_merge", axis=1)
            changed_records["_merge"] = "right_only"
            new_records = new_records.append(changed_records)

            new_records = new_records.drop("_merge", axis=1)
            new_records.columns = [col.replace("_new", "") for col in new_records.columns]

    return new_records
