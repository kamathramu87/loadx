from __future__ import annotations

from datetime import datetime

import pytest
import pyspark.sql.functions as f

from loadx import SCD2ColumnNames, SCD2Loader, SourceType
from loadx.exceptions import ConfigurationError, DataValidationError
from loadx.scd2.config import SCD2Config, internal_columns
from loadx.scd2.transforms import validate_config


@pytest.mark.parametrize(
    "name", sorted(internal_columns("snapshot_date") - {"delete_flag"})
)
def test_delete_flag_cannot_shadow_processing_columns(name):
    config = SCD2Config.create("id", scd_columns=SCD2ColumnNames(delete_flag=name))
    with pytest.raises(ConfigurationError, match="collide with internal"):
        validate_config(config)


@pytest.mark.parametrize("name", ["latest_record_flag", "delete_flag"])
def test_disabled_output_names_cannot_hide_duplicates(name):
    config = SCD2Config.create(
        "id",
        source_type=SourceType.INCREMENTAL,
        scd_columns=SCD2ColumnNames(row_hash=name),
    )
    with pytest.raises(ConfigurationError, match="Duplicate output"):
        validate_config(config)


@pytest.mark.parametrize(
    "before,after",
    [
        (("a|b", "c"), ("a", "b|c")),
        ((None, "x"), ("x", None)),
        ((None, ""), ("", "")),
    ],
)
def test_hash_preserves_boundaries_and_null_positions(spark, before, after):
    source = spark.createDataFrame(
        [(1, *before, datetime(2024, 1, 1)), (1, *after, datetime(2024, 1, 2))],
        "id long, a string, b string, snapshot_date timestamp",
    )
    result = SCD2Loader(spark).slowly_changing_dimension(source, "id")
    rows = result.orderBy("valid_from").collect()
    assert len(rows) == 2
    assert (rows[0].a, rows[0].b) == before
    assert (rows[1].a, rows[1].b) == after
    assert rows[0].row_hash != rows[1].row_hash
    assert rows[0].valid_until == rows[1].valid_from


def test_hash_is_stable_across_column_order_and_ignores_keys(spark):
    source = spark.createDataFrame(
        [(1, "A", "B", datetime(2024, 1, 1)), (2, "A", "B", datetime(2024, 1, 1))],
        "id long, a string, b string, snapshot_date timestamp",
    )
    loader = SCD2Loader(spark)
    first = loader.slowly_changing_dimension(source, "id")
    reordered = loader.slowly_changing_dimension(
        source.select(*reversed(source.columns)), "id"
    )
    assert {r.row_hash for r in first.collect()} == {
        r.row_hash for r in reordered.collect()
    }
    assert first.select("row_hash").distinct().count() == 1


def test_existing_hashes_do_not_trigger_spurious_changes(spark):
    source = spark.createDataFrame(
        [(1, "A", datetime(2024, 1, 1))],
        "id long, city string, snapshot_date timestamp",
    )
    loader = SCD2Loader(spark)
    target = loader.slowly_changing_dimension(source, "id").withColumn(
        "row_hash", f.lit("legacy-hash")
    )
    next_day = source.withColumn("snapshot_date", f.lit(datetime(2024, 1, 2)))
    result = loader.slowly_changing_dimension(next_day, "id", df_tgt=target)
    assert result.collect() == []


def test_hash_preserves_timestamp_microseconds(spark):
    source = spark.createDataFrame(
        [
            (1, datetime(2024, 1, 1, microsecond=1), datetime(2024, 1, 1)),
            (1, datetime(2024, 1, 1, microsecond=2), datetime(2024, 1, 2)),
        ],
        "id long, event_time timestamp, snapshot_date timestamp",
    )
    rows = SCD2Loader(spark).slowly_changing_dimension(source, "id").collect()
    assert len(rows) == 2
    assert rows[0].row_hash != rows[1].row_hash


def test_ignored_columns_and_key_only_sources(spark):
    source = spark.createDataFrame(
        [(1, "A", datetime(2024, 1, 1)), (1, "B", datetime(2024, 1, 2))],
        "id long, audit string, snapshot_date timestamp",
    )
    loader = SCD2Loader(spark)
    assert (
        loader.slowly_changing_dimension(source, "id", ignore_columns=["audit"]).count()
        == 1
    )
    assert loader.slowly_changing_dimension(source.drop("audit"), "id").count() == 1


@pytest.mark.parametrize(
    "key", ["n", "has_null", "count", "_validation_count", "_validation_count_"]
)
def test_validation_aliases_do_not_shadow_business_keys(spark, key):
    loader = SCD2Loader(spark)
    source = spark.createDataFrame(
        [(1, "A", datetime(2024, 1, 1))],
        f"{key} long, city string, snapshot_date timestamp",
    )
    target = loader.slowly_changing_dimension(source, key).localCheckpoint(eager=True)
    next_day = source.withColumn(
        "snapshot_date", f.lit(datetime(2024, 1, 2))
    ).withColumn("city", f.lit("B"))
    result = loader.slowly_changing_dimension(next_day, key, df_tgt=target)
    assert sorted(r.upsert_flag for r in result.collect()) == ["I", "U"]
    with pytest.raises(DataValidationError, match="more than one active"):
        loader.slowly_changing_dimension(
            next_day, key, df_tgt=target.unionByName(target)
        )
    with pytest.raises(DataValidationError, match="null"):
        loader.slowly_changing_dimension(
            source.withColumn(key, f.lit(None).cast("long")), key
        )
    with pytest.raises(DataValidationError, match="conflicting rows"):
        loader.slowly_changing_dimension(
            source.unionByName(source.withColumn("city", f.lit("B"))), key
        )
    target.unpersist()


@pytest.mark.parametrize(
    "field",
    [
        "row_hash",
        "valid_from",
        "valid_until",
        "active_flag",
        "insert_date",
        "latest_record_flag",
    ],
)
def test_renamed_outputs_preserve_same_named_source_columns(spark, field):
    names = SCD2ColumnNames(**{field: f"scd_{field}"})
    loader = SCD2Loader(spark)
    source = spark.createDataFrame(
        [(1, "A", datetime(2024, 1, 1))],
        f"id long, {field} string, snapshot_date timestamp",
    )
    target = loader.slowly_changing_dimension(
        source, "id", scd_columns=names
    ).localCheckpoint(eager=True)
    next_day = source.withColumn(
        "snapshot_date", f.lit(datetime(2024, 1, 2))
    ).withColumn(field, f.lit("B"))
    result = loader.slowly_changing_dimension(
        next_day, "id", df_tgt=target, scd_columns=names
    )
    assert sorted((r[field], r.upsert_flag) for r in result.collect()) == [
        ("A", "U"),
        ("B", "I"),
    ]
    target.unpersist()


@pytest.mark.parametrize("open_end", [None, datetime(9999, 12, 31)])
@pytest.mark.parametrize("custom_names", [False, True])
def test_reappearance_clears_previous_latest_flag(spark, open_end, custom_names):
    names = SCD2ColumnNames()
    if custom_names:
        names = SCD2ColumnNames(**{name: f"scd_{name}" for name in names.field_list()})
    loader = SCD2Loader(spark)

    def load(source, target=None):
        return loader.slowly_changing_dimension(
            source,
            ["id", "region"],
            df_tgt=target,
            open_end_date=open_end,
            scd_columns=names,
            enable_latest_record_flag=True,
        )

    def snapshot(rows):
        return spark.createDataFrame(
            rows, "id long, region string, city string, snapshot_date timestamp"
        )

    # Key 1 is deleted, while the same id in another region remains active.
    history = (
        load(
            snapshot(
                [
                    (1, "east", "A", datetime(2024, 1, 1)),
                    (1, "west", "B", datetime(2024, 1, 1)),
                    (1, "west", "B", datetime(2024, 1, 2)),
                ]
            )
        )
        .drop("upsert_flag")
        .localCheckpoint(eager=True)
    )
    original = history.filter("region = 'east'").first().asDict()
    new_source = snapshot(
        [(1, "east", "A", datetime(2024, 1, 3)), (1, "west", "B", datetime(2024, 1, 3))]
    )
    # The target contract does not require a snapshot_date column.
    changes = load(new_source, history.drop("snapshot_date")).localCheckpoint(
        eager=True
    )
    rows = changes.collect()
    assert len(rows) == 2
    update = next(row.asDict() for row in rows if row.upsert_flag == "U")
    assert update == {**original, names.latest_record_flag: False, "upsert_flag": "U"}
    match = ["id", "region", names.valid_from]
    merged = (
        history.join(changes.select(*match), match, "left_anti")
        .unionByName(changes.drop("upsert_flag"))
        .localCheckpoint(eager=True)
    )
    assert merged.filter(f.col(names.latest_record_flag)).count() == 2
    # A replay and a later unchanged snapshot must not emit a second flag update.
    assert load(new_source, merged).collect() == []
    assert (
        load(
            new_source.withColumn("snapshot_date", f.lit(datetime(2024, 1, 4))), merged
        ).collect()
        == []
    )
    for cached in (history, changes, merged):
        cached.unpersist()


def test_latest_flag_requires_target_metadata(spark):
    loader = SCD2Loader(spark)
    source = spark.createDataFrame(
        [(1, "A", datetime(2024, 1, 1))],
        "id long, city string, snapshot_date timestamp",
    )
    target = loader.slowly_changing_dimension(source, "id")
    with pytest.raises(DataValidationError, match="latest_record_flag"):
        loader.slowly_changing_dimension(
            source, "id", df_tgt=target, enable_latest_record_flag=True
        )
