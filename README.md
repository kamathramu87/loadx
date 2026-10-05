# loadx

A PySpark library for **Slowly Changing Dimension Type 2 (SCD2)** transformations.

Tracks historical changes to dimensional data using valid/until date ranges, hash-based change detection, and active/delete flags.

## Installation

```bash
pip install loadx
```

## Quick start

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

To exclude columns such as audit timestamps from change detection, pass `ignore_columns=["updated_at"]`.

## Output columns

| Column | Description |
|--------|-------------|
| `valid_from` | Date the record became active |
| `valid_until` | Date the record was superseded (`9999-12-31` = currently active) |
| `active_flag` | `True` for the current version of a record |
| `delete_flag` | `True` for records deleted from the source (full snapshots only) |
| `row_hash` | SHA-256 hash of the non-key columns, used for change detection |
| `insert_date` | Timestamp when this record version was written |
| `latest_record_flag` | `True` for the most recent record per key (when `enable_latest_record_flag=True`) |
| `upsert_flag` | `I` (insert) or `U` (update) for downstream merge operations |

## Documentation

Full documentation at [kamathramu87.github.io/loadx](https://kamathramu87.github.io/loadx).
