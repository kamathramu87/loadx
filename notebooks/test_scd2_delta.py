# Databricks notebook source
# MAGIC %md
# MAGIC # SCD2 Delta integration checks
# MAGIC This bundle job installs its built wheel, runs five scenarios, and cleans up
# MAGIC its uniquely named test tables. Any failed check fails the job.

# COMMAND ----------

# ruff: noqa: E402 -- imports must follow the notebook library installation/restart
from __future__ import annotations

from pathlib import Path

notebook_utils = globals()["dbutils"]
notebook_utils.widgets.text("wheel_dir", "")
notebook_utils.widgets.text("test_schema", "")
wheel_dir = notebook_utils.widgets.get("wheel_dir")
if not wheel_dir:
    raise ValueError("Set wheel_dir to the bundle's deployed wheel directory")
wheels = list(Path(wheel_dir).glob("loadx-*.whl"))
if len(wheels) != 1:
    raise ValueError(
        f"Expected exactly one loadx wheel in {wheel_dir}; found {len(wheels)}"
    )
wheel_file = str(wheels[0])

# Databricks provides Spark. Installing dependencies could replace its runtime client.

# COMMAND ----------

# MAGIC %pip install --force-reinstall --no-deps $wheel_file

# COMMAND ----------

globals()["dbutils"].library.restartPython()

# COMMAND ----------

import logging
from contextlib import contextmanager
from datetime import datetime
from importlib.metadata import version
from uuid import uuid4

from loadx import SCD2ColumnNames, SCD2Loader, SourceType
from loadx.scd2.delta import quote_table

logger = logging.getLogger("loadx.integration")
spark = globals()["spark"]
test_schema = globals()["dbutils"].widgets.get("test_schema")
if not test_schema:
    raise ValueError("Set test_schema to an existing writable catalog.schema")
logger.info("Testing installed loadx %s", version("loadx"))


def require(condition, message):
    if not condition:
        raise AssertionError(message)


@contextmanager
def temporary_target():
    table = f"{test_schema}.loadx_scd2_test_{uuid4().hex}"
    quoted = quote_table(table)
    try:
        yield spark, table
    finally:
        spark.sql(f"DROP TABLE IF EXISTS {quoted}").collect()


# COMMAND ----------


def test_delta_history(target, custom, incremental):
    spark, table = target
    names = SCD2ColumnNames()
    if custom:
        names = SCD2ColumnNames(
            **{field: f"scd_{field}" for field in names.field_list()}
        )
    loader = SCD2Loader(spark)
    source_type = SourceType.INCREMENTAL if incremental else SourceType.FULL

    def load(day, rows):
        source = spark.createDataFrame(
            [(key, region, city, datetime(2024, 1, day)) for key, region, city in rows],
            "id long, region string, city string, snapshot_date timestamp",
        )
        return loader.load(
            source,
            table,
            ["id", "region"],
            scd_columns=names,
            enable_latest_record_flag=True,
            source_type=source_type,
            open_end_date=None if custom else datetime(9999, 12, 31),
        )

    initial = load(1, [(1, "east", "A"), (1, "west", "B"), (2, "east", "C")])
    require(initial.count() == 3, "initial.count() == 3")
    require(
        "upsert_flag" not in initial.columns, "'upsert_flag' not in initial.columns"
    )
    original_west = initial.filter("id = 1 AND region = 'west'").first().asDict()
    next_rows = [(1, "east", "D"), (1, "west", "B"), (3, "east", "E")]
    history = load(2, next_rows)
    require(history.count() == 5, "history.count() == 5")
    east = (
        history.filter("id = 1 AND region = 'east'").orderBy(names.valid_from).collect()
    )
    require(
        [r.city for r in east] == ["A", "D"], "[r.city for r in east] == ['A', 'D']"
    )
    require(
        east[0][names.valid_until] == datetime(2024, 1, 2),
        "east[0][names.valid_until] == datetime(2024, 1, 2)",
    )
    require(not east[0][names.active_flag], "not east[0][names.active_flag]")
    require(east[1][names.active_flag], "east[1][names.active_flag]")
    require(
        history.filter("id = 1 AND region = 'west'").first().asDict() == original_west,
        "history.filter(\"id = 1 AND region = 'west'\").first().asDict() == original_west",
    )
    absent = history.filter("id = 2").first()
    require(
        absent[names.active_flag] == incremental,
        "absent[names.active_flag] == incremental",
    )
    if not incremental:
        require(absent[names.delete_flag], "absent[names.delete_flag]")
    else:
        require(
            names.delete_flag not in history.columns,
            "names.delete_flag not in history.columns",
        )
    before = {tuple(r) for r in history.collect()}
    require(
        {tuple(r) for r in load(2, next_rows).collect()} == before,
        "{tuple(r) for r in load(2, next_rows).collect()} == before",
    )
    final = load(3, [*next_rows, (2, "east", "C")])
    require(
        final.count() == (5 if incremental else 6),
        "final.count() == (5 if incremental else 6)",
    )
    require(
        final.filter(names.latest_record_flag).count() == 4,
        "final.filter(names.latest_record_flag).count() == 4",
    )
    if not incremental:
        old = final.filter("id = 2").orderBy(names.valid_from).first()
        require(not old[names.latest_record_flag], "not old[names.latest_record_flag]")


def test_existing_empty_target_without_snapshot_date_and_invalid_batch(target):
    from loadx.exceptions import DataValidationError

    spark, table = target
    loader = SCD2Loader(spark)

    def source(day, cities):
        return spark.createDataFrame(
            [(1, city, datetime(2024, 1, day)) for city in cities],
            "id long, city string, snapshot_date timestamp",
        )

    empty = (
        loader.slowly_changing_dimension(
            source(1, ["A"]), "id", source_type=SourceType.INCREMENTAL
        )
        .limit(0)
        .drop("snapshot_date", "upsert_flag")
    )
    empty.write.format("delta").mode("error").saveAsTable(quote_table(table))
    loader.load(source(1, ["A"]), table, "id", source_type=SourceType.INCREMENTAL)
    history = loader.load(
        source(2, ["B"]), table, "id", source_type=SourceType.INCREMENTAL
    )
    require(
        "snapshot_date" not in history.columns, "'snapshot_date' not in history.columns"
    )
    rows = history.orderBy("valid_from").collect()
    require(
        [r.city for r in rows] == ["A", "B"], "[r.city for r in rows] == ['A', 'B']"
    )
    require(
        rows[0].valid_until == datetime(2024, 1, 2),
        "rows[0].valid_until == datetime(2024, 1, 2)",
    )
    before = {tuple(r) for r in rows}
    try:
        loader.load(
            source(3, ["C", "D"]), table, "id", source_type=SourceType.INCREMENTAL
        )
    except DataValidationError as error:
        require("conflicting rows" in str(error), "Expected conflicting-row validation")
    else:
        raise AssertionError("Conflicting source rows were accepted")
    require(
        {tuple(r) for r in spark.table(quote_table(table)).collect()} == before,
        "{tuple(r) for r in spark.table(quote_table(table)).collect()} == before",
    )


# COMMAND ----------

results = []
for incremental in (False, True):
    for custom in (False, True):
        scenario = f"history: incremental={incremental}, custom_metadata={custom}"
        with temporary_target() as target:
            test_delta_history(target, custom, incremental)
        results.append((scenario, "PASSED"))
        logger.info("PASSED: %s", scenario)

with temporary_target() as target:
    test_existing_empty_target_without_snapshot_date_and_invalid_batch(target)
results.append(("empty target, optional snapshot_date, conflicting input", "PASSED"))
globals()["display"](spark.createDataFrame(results, "scenario string, result string"))
globals()["dbutils"].notebook.exit(
    f"All {len(results)} SCD2 Delta integration scenarios passed"
)
