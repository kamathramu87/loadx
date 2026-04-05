# scd-loader

A PySpark library for implementing **Slowly Changing Dimension Type 2 (SCD2)** transformations. It tracks historical changes to dimensional data using valid_from/valid_until date ranges, SHA-256 hash-based change detection, and active/delete flags.

## Installation

```bash
pip install scd-loader
```

## Quick Start

```python
from loadx import SCD2Loader, SCD2ColumnNames, SourceType

loader = SCD2Loader(spark_session=spark)

result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
)
```


### Incremental Load

Pass a `df_tgt` to merge new snapshots into an existing SCD2 table:

```python
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    df_tgt=target_df,
)
```

### Source Type

Use `SourceType.FULL` (default) when the source is a complete daily/periodic snapshot — records absent from the latest snapshot are detected as deletions and `delete_flag` is included in the output:

```python
from loadx import SCD2Loader, SourceType

result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    source_type=SourceType.FULL,  # default
)
```

Use `SourceType.INCREMENTAL` when the source only contains new or changed records — deletion detection is skipped and `delete_flag` is omitted from the output:

```python
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    source_type=SourceType.INCREMENTAL,
)
```

### Latest Record Flag

Add a `latest_record_flag` column to identify the most recent record per business key across all historical versions:

```python
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    enable_latest_record_flag=True,
)
```

## API Reference

### `SCD2Loader(spark_session=None)`

| Parameter | Type | Description |
|---|---|---|
| `spark_session` | `SparkSession \| None` | Existing SparkSession. Creates one if not provided. |

### `slowly_changing_dimension(...)`

| Parameter | Type | Default | Description |
|---|---|---|---|
| `df_src` | `DataFrame` | — | Source snapshot DataFrame |
| `business_keys` | `list[str] \| str` | — | Columns that uniquely identify a dimension row |
| `date_column` | `str` | `snapshot_date` | Column containing the snapshot date |
| `df_tgt` | `DataFrame \| None` | `None` | Existing SCD2 target for incremental loads |
| `ignore_columns` | `list[str] \| None` | `None` | Columns excluded from hash-based change detection |
| `non_copy_fields` | `list[str] \| None` | `None` | Source columns excluded from the output |
| `open_end_date` | `datetime \| None` | `9999-12-31` | `valid_until` value for currently active records |
| `scd_columns` | `SCD2ColumnNames \| dict[str, str] \| None` | `None` | Override default SCD2 output column names |
| `enable_latest_record_flag` | `bool` | `False` | When `True`, adds a `latest_record_flag` column marking the most recent record per business key |
| `source_type` | `SourceType` | `SourceType.FULL` | `FULL` for complete snapshots (deletion detection enabled, `delete_flag` included). `INCREMENTAL` for feeds that only contain new/changed records (deletion detection skipped, `delete_flag` omitted) |

## Output Columns

The resulting DataFrame includes all source columns plus:

| Column | Default Name | Description |
|---|---|---|
| `valid_from` | `valid_from` | Date when the record became active |
| `valid_until` | `valid_until` | Date when the record was superseded (`9999-12-31` if still active) |
| `active_flag` | `active_flag` | `True` for the currently active version of a record |
| `delete_flag` | `delete_flag` | `True` if the record was deleted in the source. Only present when `source_type=SourceType.FULL` |
| `row_hash` | `row_hash` | SHA-256 hash of non-key columns (excluding `ignore_columns`) |
| `insert_date` | `insert_date` | Timestamp when this record version was written to the target table |
| `upsert_flag` | `upsert_flag` | `I` for inserts, `U` for updates |
| `latest_record_flag` | `latest_record_flag` | `True` for the most recent record per business key. Only present when `enable_latest_record_flag=True` |

Column names can be overridden via the `scd_columns` parameter using either `SCD2ColumnNames` (recommended — full IDE type hints) or a plain dict:

```python
from loadx import SCD2Loader, SCD2ColumnNames

# Using SCD2ColumnNames (recommended)
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    scd_columns=SCD2ColumnNames(
        valid_from="eff_start_date",
        valid_until="eff_end_date",
        active_flag="is_current",
    ),
)

# Using a dict (also supported)
result_df = loader.slowly_changing_dimension(
    df_src=source_df,
    business_keys=["employee_id"],
    scd_columns={
        "valid_from": "eff_start_date",
        "valid_until": "eff_end_date",
        "active_flag": "is_current",
    },
)
```

## Exceptions

| Exception | Raised When |
|---|---|
| `EmptyDataExceptionError` | Source DataFrame is empty |
| `OldDataExceptionError` | Source snapshot date is older than the latest target date |
| `BusinessKeysEmptyError` | No business keys provided |
| `ConfigurationError` | Invalid configuration values |
| `DataValidationError` | Required columns are missing from the source |
