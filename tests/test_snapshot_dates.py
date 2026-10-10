from __future__ import annotations

from datetime import date, datetime, timezone

import pytest
from pyspark.sql import functions as f
from pyspark.sql.types import TimestampType

from loadx import SCD2Loader, SourceType
from loadx.exceptions import DataValidationError, OldDataExceptionError


@pytest.fixture
def date_spark(spark):
    session = spark.newSession()
    session.conf.set("spark.sql.session.timeZone", "UTC")
    session.conf.set("spark.sql.shuffle.partitions", "1")
    return session


@pytest.mark.parametrize(
    "data_type,day1,day2",
    [
        ("date", date(2024, 1, 1), date(2024, 1, 2)),
        (
            "timestamp",
            datetime(2024, 1, 1, tzinfo=timezone.utc),
            datetime(2024, 1, 2, tzinfo=timezone.utc),
        ),
        ("timestamp_ntz", datetime(2024, 1, 1), datetime(2024, 1, 2)),
        ("string", "2024-01-01", "2024-01-02T00:00:00Z"),
    ],
)
@pytest.mark.parametrize("source_type", [SourceType.FULL, SourceType.INCREMENTAL])
def test_initial_and_incremental_dates(date_spark, data_type, day1, day2, source_type):
    schema = f"id long, city string, observed_at {data_type}"
    first = date_spark.createDataFrame([(1, "A", day1), (2, "X", day1)], schema)
    second = date_spark.createDataFrame([(1, "B", day2)], schema)
    loader = SCD2Loader(date_spark)
    options = dict(
        date_column="observed_at", source_type=source_type, open_end_date=None
    )
    target = loader.slowly_changing_dimension(first, "id", **options).localCheckpoint(
        eager=True
    )
    try:
        result = loader.slowly_changing_dimension(
            second, "id", df_tgt=target, **options
        )
        for frame in (target, result):
            for column in ("observed_at", "valid_from", "valid_until"):
                assert isinstance(frame.schema[column].dataType, TimestampType)
        rows = sorted(
            (r.id, r.city, r.valid_from, r.valid_until, r.active_flag, r.upsert_flag)
            for r in result.select(
                "id",
                "city",
                f.col("valid_from").cast("long").alias("valid_from"),
                f.col("valid_until").cast("long").alias("valid_until"),
                "active_flag",
                "upsert_flag",
            ).collect()
        )
        expected = [
            (1, "A", 1704067200, 1704153600, False, "U"),
            (1, "B", 1704153600, None, True, "I"),
        ]
        if source_type == SourceType.FULL:
            expected.append((2, "X", 1704067200, 1704153600, False, "U"))
        assert rows == expected
    finally:
        target.unpersist()


@pytest.mark.parametrize("ansi", ["true", "false"])
@pytest.mark.parametrize("bad_date", ["bad-date", "2024-02-30", "", "   "])
def test_invalid_strings_fail_before_null_validation(date_spark, ansi, bad_date):
    date_spark.conf.set("spark.sql.ansi.enabled", ansi)
    source = date_spark.createDataFrame(
        [(1, "A", None), (2, "B", bad_date)],
        "id long, city string, snapshot_date string",
    )
    with pytest.raises(DataValidationError, match="snapshot_date.*cannot be parsed"):
        SCD2Loader(date_spark).slowly_changing_dimension(source, "id")


@pytest.mark.parametrize(
    "data_type,value",
    [
        ("long", 1704067200),
        ("double", 1704067200.5),
        ("boolean", True),
        ("array<string>", ["2024-01-01"]),
    ],
)
def test_unsupported_snapshot_types_are_rejected(date_spark, data_type, value):
    source = date_spark.createDataFrame(
        [(1, "A", value)], f"id long, city string, snapshot_date {data_type}"
    )
    with pytest.raises(DataValidationError, match="snapshot_date.*must be date"):
        SCD2Loader(date_spark).slowly_changing_dimension(source, "id")


@pytest.mark.parametrize("data_type", ["date", "string", "timestamp", "timestamp_ntz"])
def test_null_dates_still_fail_row_validation(date_spark, data_type):
    source = date_spark.createDataFrame(
        [(1, "A", None)], f"id long, city string, snapshot_date {data_type}"
    )
    with pytest.raises(DataValidationError, match="null"):
        SCD2Loader(date_spark).slowly_changing_dimension(source, "id")


@pytest.mark.parametrize("conflicting", [False, True])
def test_equivalent_strings_are_deduplicated_after_normalization(
    date_spark, conflicting
):
    source = date_spark.createDataFrame(
        [
            (1, "A", "2024-01-01"),
            (1, "B" if conflicting else "A", "2024-01-01T02:00:00+02:00"),
        ],
        "id long, city string, snapshot_date string",
    )
    loader = SCD2Loader(date_spark)
    if conflicting:
        with pytest.raises(DataValidationError, match="conflicting rows"):
            loader.slowly_changing_dimension(source, "id")
    else:
        assert loader.slowly_changing_dimension(source, "id").count() == 1


def test_offset_dates_order_by_instant_with_microsecond_precision(date_spark):
    source = date_spark.createDataFrame(
        [
            (1, "later", "2024-01-01T00:00:00.000002Z"),
            (1, "earlier", "2024-01-01T01:00:00.000001+01:00"),
        ],
        "id long, city string, snapshot_date string",
    )
    rows = (
        SCD2Loader(date_spark)
        .slowly_changing_dimension(source, "id")
        .orderBy("valid_from")
        .collect()
    )
    assert [row.city for row in rows] == ["earlier", "later"]
    assert rows[0].valid_from.timestamp() == 1704067200.000001
    assert rows[0].valid_until == rows[1].valid_from
    assert rows[1].valid_from.timestamp() == 1704067200.000002


@pytest.mark.parametrize(
    "data_type,value",
    [
        ("date", date(2024, 1, 1)),
        ("timestamp_ntz", datetime(2024, 1, 1)),
        ("string", "2024-01-01"),
    ],
)
def test_naive_dates_use_session_timezone(date_spark, data_type, value):
    date_spark.conf.set("spark.sql.session.timeZone", "Asia/Kolkata")
    # Output remains TimestampType even if the session's preferred type is NTZ.
    date_spark.conf.set("spark.sql.timestampType", "TIMESTAMP_NTZ")
    source = date_spark.createDataFrame(
        [(1, value)], f"id long, snapshot_date {data_type}"
    )
    output = SCD2Loader(date_spark).slowly_changing_dimension(source, "id")
    for column in ("snapshot_date", "valid_from", "valid_until"):
        assert isinstance(output.schema[column].dataType, TimestampType)
    expected_epoch = 1704047400  # 2023-12-31 18:30:00 UTC = 2024-01-01 midnight IST
    assert (
        output.select(f.col("snapshot_date").cast("long")).first()[0] == expected_epoch
    )


def test_normalized_strings_retain_stale_source_protection(date_spark):
    loader = SCD2Loader(date_spark)
    target = loader.slowly_changing_dimension(
        date_spark.createDataFrame(
            [(1, "2024-01-02")], "id long, snapshot_date string"
        ),
        "id",
    )
    old = date_spark.createDataFrame(
        [(1, "2024-01-01")], "id long, snapshot_date string"
    )
    with pytest.raises(OldDataExceptionError):
        loader.slowly_changing_dimension(old, "id", df_tgt=target)
