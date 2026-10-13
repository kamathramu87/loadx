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

Bob is unchanged, so he does not appear. Do not overwrite the target with this output.

## Load directly into Delta (Databricks)

`load()` handles transformation and persistence together. It creates a missing
Delta table and merges subsequent batches on the business keys plus `valid_from`.

```python
loader.load(day1, "catalog.schema.dim_employee", "employee_id")
history = loader.load(day2, "catalog.schema.dim_employee", "employee_id")
```

It returns the complete persisted table. Matched `U` operations update existing
versions; unmatched `I` operations insert versions. Unchanged rows and closed
history are retained. New tables omit the internal `upsert_flag` column.
All transformation options, including custom SCD column names, nullable open-end
dates, `enable_latest_record_flag`, and `source_type`, are supported.

Use a Delta-enabled Spark session and an existing schema with create/write
permissions. Pass an unquoted table, schema.table, or catalog.schema.table name.
Existing targets must have compatible business and SCD2 columns; their
`snapshot_date` column is optional. Schema evolution is not performed. Serialize
writers for each dimension because transformation reads precede the atomic merge.
Replayed batches follow the existing date policy; stale batches can raise
`OldDataExceptionError`. Empty sources retain the transformer's validation error.
The merge follows [Databricks MERGE semantics](https://docs.databricks.com/aws/en/delta/merge).

### Databricks bundle integration job

The Databricks Asset Bundle in `databricks.yml` builds this checkout into a wheel
and deploys the `scd2_integration` job. Its serverless notebook installs that wheel
with `--no-deps` (Databricks supplies Spark), restarts Python, and runs five
integration scenarios directly. It does not invoke pytest or import the source
checkout. Failed checks raise exceptions and fail the job.

With a current Databricks CLI, `uv`, and workspace authentication configured:

```bash
databricks bundle validate -t dev
databricks bundle deploy -t dev
databricks bundle run -t dev scd2_integration
```

The `dev` target uses the Azure workspace configured in `databricks.yml` and
`lakehouse_dev.bronze`. Override the scratch schema at deployment:

```bash
databricks bundle deploy -t dev --var test_schema=catalog.scratch_schema
databricks bundle run -t dev scd2_integration
```

Deployment builds a fresh wheel in `dist/bundle/` and synchronizes it with the
notebook. Library dependencies are intentionally not reinstalled in Databricks,
and source files in `loadx/` are excluded from workspace sync to ensure the tests
exercise the installed package. The notebook's `wheel_dir` and `test_schema`
parameters are populated by the bundle job.

Each scenario creates a unique `loadx_scd2_test_<uuid>` table and drops only that
table in cleanup. Checks cover initial creation, version updates, unchanged rows,
composite keys, replay, deletion and reappearance, incremental feeds, custom
metadata, null open-end dates, pre-created empty targets, and invalid input.
The final notebook cell displays a results table. An interrupted job may leave
one of its test tables for manual cleanup.

The test schema must already exist and its backing storage must permit table
creation and writes. The job does not create schemas or change storage permissions.


To exclude columns such as audit timestamps from change detection, pass `ignore_columns=["updated_at"]`.

Snapshot dates can be Spark dates, timestamps, timestamps without a time zone, or
strings parseable as timestamps. They are normalized to `TimestampType` before
validation and ordering. Dates and values without an explicit offset use the
Spark session time zone; malformed strings and unsupported types raise
`DataValidationError`. See [input validation](docs/index.md#input-validation).

With `enable_latest_record_flag=True`, pass the full target history with all SCD2
columns. A reappearing key emits an update clearing the previous closed version's
latest flag as well as an insert for the new version.

Hashes now use structured JSON with explicit nulls, excluding business keys and
ignored columns. This changes stored hash values from earlier releases; see
[hash compatibility](docs/index.md#hash-compatibility) before comparing persisted
hashes outside the loader.

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
