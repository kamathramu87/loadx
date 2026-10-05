# loadx

A PySpark library for implementing **Slowly Changing Dimension Type 2 (SCD2)** transformations. It tracks historical changes to dimensional data using valid_from/valid_until date ranges, SHA-256 hash-based change detection, and active/delete flags.

## Installation

```bash
pip install loadx
```

## Quick Start

The example below is self-contained. It runs an initial load, then an incremental load against the result.

```python
from datetime import datetime

from pyspark.sql import SparkSession

from loadx import SCD2Loader

spark = SparkSession.builder.master("local[1]").getOrCreate()
loader = SCD2Loader(spark_session=spark)

columns = ["employee_id", "name", "department", "snapshot_date"]

# Day 1: initial full snapshot
day1 = spark.createDataFrame(
    [
        (1, "Alice", "Engineering", datetime(2024, 1, 1)),
        (2, "Bob", "Sales", datetime(2024, 1, 1)),
    ],
    columns,
)

target = loader.slowly_changing_dimension(
    df_src=day1,
    business_keys=["employee_id"],
    date_column="snapshot_date",
)
target.show()

# Day 2: Alice moves to Marketing, Bob is unchanged, Carol is new
day2 = spark.createDataFrame(
    [
        (1, "Alice", "Marketing", datetime(2024, 1, 2)),
        (2, "Bob", "Sales", datetime(2024, 1, 2)),
        (3, "Carol", "Finance", datetime(2024, 1, 2)),
    ],
    columns,
)

changes = loader.slowly_changing_dimension(
    df_src=day2,
    business_keys=["employee_id"],
    date_column="snapshot_date",
    df_tgt=target,
)
changes.show()
```

### Incremental Load

The first call has no target, so every row is an insert (`upsert_flag = "I"`) and the result is the complete initial table.

**With `df_tgt`, the output is a set of merge operations, not the complete target table.** It contains only the rows that need to change in the target:

| employee_id | department | valid_from | valid_until | active_flag | upsert_flag |
|---|---|---|---|---|---|
| 1 | Engineering | 2024-01-01 | 2024-01-02 | `False` | `U` — close the existing version |
| 1 | Marketing | 2024-01-02 | 9999-12-31 | `True` | `I` — insert the new version |
| 3 | Finance | 2024-01-02 | 9999-12-31 | `True` | `I` — insert the new key |

Bob is unchanged, so he does not appear. Do not overwrite the target with this output, or unchanged records will be lost. Apply it with a merge keyed on the business keys plus `valid_from`: update matching rows where `upsert_flag = "U"`, and insert rows where `upsert_flag = "I"`. For example, with Delta Lake:

```python
from delta.tables import DeltaTable

(
    DeltaTable.forName(spark, "dim_employee").alias("t")
    .merge(
        changes.alias("s"),
        "t.employee_id = s.employee_id AND t.valid_from = s.valid_from",
    )
    .whenMatchedUpdateAll(condition="s.upsert_flag = 'U'")
    .whenNotMatchedInsertAll(condition="s.upsert_flag = 'I'")
    .execute()
)
```

`upsert_flag` is included in the output, so drop it before merging if the target table has no such column.

#### Target contract

`df_tgt` is the SCD2 table built from earlier loads, after their output has been merged. You can pass the full table, including closed history.

| Rule | Detail |
|---|---|
| Required columns | The retained source attributes plus `valid_from` and `valid_until`; `snapshot_date` is optional in the target. With `enable_latest_record_flag=True`, pass the full history with all configured SCD2 output columns. |
| Eligible records | Current records, where `valid_until` equals `open_end_date` (or is null when `open_end_date=None`). A reappearing key also emits a `U` row clearing its previous closed version's latest flag, when enabled. Historical dates and attributes are preserved. |
| One current record per key | A target with more than one current record for a business key is rejected with `DataValidationError`. |
| Freshness | A source older than the latest target `valid_from` raises `OldDataExceptionError`. Source rows dated on or before that date are ignored. |
| Applying the output | Merge on business keys plus `valid_from`: update `U` rows, insert `I` rows. |

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

Use this option consistently from the initial load onward. For incremental loads,
pass the complete target history, including its SCD2 metadata. When a deleted key
reappears, merge both the new insert and the update clearing the old latest flag.
That historical update preserves the existing dates, attributes, hash and insert timestamp.

### Hash compatibility

`row_hash` uses SHA-256 over JSON with sorted attribute names and explicit nulls.
Business keys, the snapshot date and `ignore_columns` are excluded. Field boundaries
are preserved, so delimiters inside values and null positions cannot hide changes.
Keep attribute names and types consistent between loads.

This format changes hash values produced by earlier delimiter-based releases.
The loader recomputes hashes from attributes on both sides of an incremental load,
so existing hashes do not cause artificial versions. Unchanged target rows retain
their stored hashes until rewritten. Consumers that compare stored hashes outside
the loader should backfill `row_hash` using the new format before comparing them.

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
| `df_tgt` | `DataFrame \| None` | `None` | Existing SCD2 table for incremental loads. See [Target contract](#target-contract) |
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

## Input Validation

Invalid input raises an error before any processing, so it can't silently change the SCD2 history:

- `source_type` must be `"full"` or `"incremental"` (or a `SourceType` member).
- `date_column` cannot be a business key, and `non_copy_fields` cannot include business keys or `date_column`.
- SCD2 output column names (including `upsert_flag`) must be unique and must not reuse internal processing names such as `deleted`.
- Every column in `ignore_columns` and `non_copy_fields` must exist in the source.
- Source columns must not reuse SCD2 output names (`valid_from`, `row_hash`, `upsert_flag`, ...) or internal names. Rename them, list them in `non_copy_fields`, or rename the output columns via `scd_columns`.
- Business keys and `date_column` must not be null.
- Each business key can have at most one row per snapshot date. Exact duplicate rows are allowed and collapsed; rows with the same key and date but different values are rejected, because there is no deterministic way to choose between them.
- `df_tgt` must satisfy the [target contract](#target-contract).

## Exceptions

All exceptions below are subclasses of `ValueError` except `EmptyDataExceptionError` and `OldDataExceptionError`.

| Exception | Raised When |
|---|---|
| `EmptyDataExceptionError` | Source DataFrame is empty |
| `OldDataExceptionError` | Source snapshot date is older than the latest target date |
| `BusinessKeysEmptyError` | No business keys provided |
| `ConfigurationError` | Invalid parameters, such as an unknown `source_type` or colliding output column names |
| `DataValidationError` | Invalid source or target data: missing or reserved columns, null keys or dates, conflicting rows for the same key and snapshot date, or duplicate active target records |
