from __future__ import annotations

from datetime import datetime

from loadx.exceptions import (
    ConfigurationError,
    DataValidationError,
    EmptyDataExceptionError,
    OldDataExceptionError,
)
import pytest
from chispa.dataframe_comparer import assert_df_equality

from loadx.scd2.loader import SCD2Loader
from loadx import SourceType


@pytest.fixture(scope="session")
def data_day1(spark_session):
    data = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-01", "%Y-%m-%d"),
        }
    ]
    df = spark_session.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day2(spark_session):
    data = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-02", "%Y-%m-%d"),
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "amsterdam",
            "country": "netherlands",
            "snapshot_date": datetime.strptime("2022-01-02", "%Y-%m-%d"),
        },
    ]
    df = spark_session.createDataFrame(data)
    return df


@pytest.fixture(scope="session")
def data_day3(spark_session):
    data = [
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "mangalore",
            "country": "india",
            "snapshot_date": datetime.strptime("2022-01-03", "%Y-%m-%d"),
        },
        {
            "employee_id": 300,
            "first_name": "akansha",
            "last_name": "sahoo",
            "place": "bangalore",
            "country": "india",
            "snapshot_date": datetime.strptime("2022-01-03", "%Y-%m-%d"),
        },
    ]
    df = spark_session.createDataFrame(data)
    return df


class TestSCD2Load:
    def test_scd2_initial_load_null_end_date(
        self,
        data_day1,
        data_day2,
        primary_key,
        date_attribute,
        expected_data_null_end_date,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
            open_end_date=None,
        )
        assert_df_equality(
            expected_data_null_end_date,
            output_df.select(expected_data_null_end_date.columns),
            ignore_nullable=True,
        )

    def test_scd2_initial_load_open_end_date(
        self,
        data_day1,
        data_day2,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2),
            date_column=date_attribute,
            business_keys=primary_key,
        )
        assert_df_equality(
            expected_data_open_end_date,
            output_df.select(expected_data_open_end_date.columns),
            ignore_nullable=True,
        )

    def test_scd2_target_exist_cacthup_one_day(
        self,
        data_day1,
        data_day2,
        data_day3,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
        expected_data_catchup_one_day,
    ):
        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=data_day1.union(data_day2).union(data_day3),
            df_tgt=expected_data_open_end_date,
            date_column=date_attribute,
            business_keys=primary_key,
        )
        assert_df_equality(
            expected_data_catchup_one_day,
            output_df.select(expected_data_catchup_one_day.columns),
            ignore_nullable=True,
        )

    def test_no_data_exception(self, data_day1, primary_key, date_attribute):
        scd = SCD2Loader()
        with pytest.raises(EmptyDataExceptionError) as msg:
            scd.slowly_changing_dimension(
                df_src=data_day1.filter("1=0"),
                date_column=date_attribute,
                business_keys=primary_key,
            )
        assert str(msg.value) == "Empty dataframe, exiting scd2 load"

    def test_old_data_exception(
        self,
        data_day1,
        primary_key,
        date_attribute,
        expected_data_open_end_date,
    ):
        scd = SCD2Loader()
        with pytest.raises(OldDataExceptionError) as msg:
            scd.slowly_changing_dimension(
                df_src=data_day1,
                df_tgt=expected_data_open_end_date,
                date_column=date_attribute,
                business_keys=primary_key,
            )
        assert str(msg.value) == "Data is older than last target load date"

    def test_latest_record_flag(self, spark_session):
        data = [
            {
                "region": "US",
                "product": "A",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "US",
                "product": "A",
                "version": 2,
                "snapshot_date": datetime(2022, 2, 1),
            },
            {
                "region": "US",
                "product": "B",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "EU",
                "product": "A",
                "version": 1,
                "snapshot_date": datetime(2022, 1, 1),
            },
            {
                "region": "EU",
                "product": "A",
                "version": 2,
                "snapshot_date": datetime(2022, 3, 1),
            },
        ]
        df_src = spark_session.createDataFrame(data)

        scd = SCD2Loader()
        output_df = scd.slowly_changing_dimension(
            df_src=df_src,
            business_keys=["region", "product"],
            date_column="snapshot_date",
            enable_latest_record_flag=True,
        )

        # Each business key combination should have exactly one latest_record_flag=True
        latest_counts = (
            output_df.filter("latest_record_flag = true")
            .groupBy("region", "product")
            .count()
        )
        assert latest_counts.count() == 3
        for row in latest_counts.collect():
            assert row["count"] == 1

        # Verify correct records are flagged as latest
        latest_records = output_df.filter("latest_record_flag = true").collect()
        latest_versions = {
            (r["region"], r["product"]): r["version"] for r in latest_records
        }
        assert latest_versions[("US", "A")] == 2
        assert latest_versions[("US", "B")] == 1
        assert latest_versions[("EU", "A")] == 2


class TestSCD2IncrementalSourceType:
    def test_incremental_initial_load(self, spark_session):
        """Incremental initial load: changed records produce SCD2 history, absent records
        stay active, and delete_flag is absent from output."""
        day1 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Amsterdam",
                    "snapshot_date": datetime(2022, 1, 1),
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "London",
                    "snapshot_date": datetime(2022, 1, 1),
                },
            ]
        )
        day2 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "Paris",
                    "snapshot_date": datetime(2022, 1, 2),
                },
            ]
        )

        output = SCD2Loader().slowly_changing_dimension(
            df_src=day1.union(day2),
            business_keys="employee_id",
            source_type=SourceType.INCREMENTAL,
        )

        assert "delete_flag" not in output.columns

        expected = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Amsterdam",
                    "valid_from": datetime(2022, 1, 1),
                    "valid_until": datetime(9999, 12, 31),
                    "active_flag": True,
                    "upsert_flag": "I",
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "London",
                    "valid_from": datetime(2022, 1, 1),
                    "valid_until": datetime(2022, 1, 2),
                    "active_flag": False,
                    "upsert_flag": "I",
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "Paris",
                    "valid_from": datetime(2022, 1, 2),
                    "valid_until": datetime(9999, 12, 31),
                    "active_flag": True,
                    "upsert_flag": "I",
                },
            ]
        )
        assert_df_equality(
            expected,
            output.select(expected.columns),
            ignore_row_order=True,
            ignore_nullable=True,
        )

    def test_incremental_with_existing_target(self, spark_session):
        """Incremental load against an existing target: only changed records are returned,
        delete_flag is absent, and SCD2 history is correct."""
        day1 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 100,
                    "name": "Alice",
                    "city": "Amsterdam",
                    "snapshot_date": datetime(2022, 1, 1),
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "London",
                    "snapshot_date": datetime(2022, 1, 1),
                },
            ]
        )
        day2 = spark_session.createDataFrame(
            [
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "Paris",
                    "snapshot_date": datetime(2022, 1, 2),
                },
            ]
        )

        scd = SCD2Loader()
        target = scd.slowly_changing_dimension(
            df_src=day1, business_keys="employee_id", source_type=SourceType.INCREMENTAL
        )
        output = scd.slowly_changing_dimension(
            df_src=day1.union(day2),
            df_tgt=target,
            business_keys="employee_id",
            source_type=SourceType.INCREMENTAL,
        )

        assert "delete_flag" not in output.columns

        expected = spark_session.createDataFrame(
            [
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "London",
                    "valid_from": datetime(2022, 1, 1),
                    "valid_until": datetime(2022, 1, 2),
                    "active_flag": False,
                    "upsert_flag": "U",
                },
                {
                    "employee_id": 200,
                    "name": "Bob",
                    "city": "Paris",
                    "valid_from": datetime(2022, 1, 2),
                    "valid_until": datetime(9999, 12, 31),
                    "active_flag": True,
                    "upsert_flag": "I",
                },
            ]
        )
        assert_df_equality(
            expected,
            output.select(expected.columns),
            ignore_row_order=True,
            ignore_nullable=True,
        )


SCHEMA = "id long, city string, snapshot_date timestamp"


class TestInputValidation:
    """Invalid inputs must raise instead of silently changing the SCD2 result."""

    @pytest.fixture
    def scd(self, spark_session):
        return SCD2Loader(spark_session)

    @pytest.fixture
    def day1(self, spark_session):
        return spark_session.createDataFrame(
            [(1, "A", datetime(2024, 1, 1)), (2, "B", datetime(2024, 1, 1))], SCHEMA
        )

    def test_invalid_source_type(self, scd, day1):
        with pytest.raises(ConfigurationError, match="source_type"):
            scd.slowly_changing_dimension(
                df_src=day1, business_keys="id", source_type="typo"
            )

    def test_source_column_uses_internal_name(self, scd, day1):
        df = day1.withColumn("deleted", day1.city)
        with pytest.raises(DataValidationError, match=r"reserved.*\['deleted'\]"):
            scd.slowly_changing_dimension(df_src=df, business_keys="id")

    def test_source_column_uses_output_name(self, scd, day1):
        df = day1.withColumn("row_hash", day1.city)
        with pytest.raises(DataValidationError, match=r"\['row_hash'\]"):
            scd.slowly_changing_dimension(df_src=df, business_keys="id")

    def test_reserved_source_column_allowed_when_not_copied(self, scd, day1):
        df = day1.withColumn("deleted", day1.city)
        out = scd.slowly_changing_dimension(
            df_src=df, business_keys="id", non_copy_fields=["deleted"]
        )
        assert out.count() == 2

    @pytest.mark.parametrize("param", ["ignore_columns", "non_copy_fields"])
    def test_unknown_column_reference(self, scd, day1, param):
        with pytest.raises(
            DataValidationError, match=rf"{param} not found.*'updated_ad'"
        ):
            scd.slowly_changing_dimension(
                df_src=day1, business_keys="id", **{param: ["updated_ad"]}
            )

    def test_null_business_key(self, scd, day1, spark_session):
        df = day1.union(
            spark_session.createDataFrame([(None, "Z", datetime(2024, 1, 1))], SCHEMA)
        )
        with pytest.raises(DataValidationError, match="1 source row.*null"):
            scd.slowly_changing_dimension(df_src=df, business_keys="id")

    def test_null_snapshot_date(self, scd, day1, spark_session):
        df = day1.union(spark_session.createDataFrame([(3, "Z", None)], SCHEMA))
        with pytest.raises(DataValidationError, match="null"):
            scd.slowly_changing_dimension(df_src=df, business_keys="id")

    def test_conflicting_rows_same_key_and_date(self, scd, day1, spark_session):
        df = day1.union(
            spark_session.createDataFrame([(1, "Q", datetime(2024, 1, 1))], SCHEMA)
        )
        with pytest.raises(DataValidationError, match=r"1 business key.*'id': 1"):
            scd.slowly_changing_dimension(df_src=df, business_keys="id")

    def test_rows_differing_only_in_non_copy_fields_are_not_conflicts(self, scd, day1):
        import pyspark.sql.functions as f

        df = day1.withColumn("batch", f.lit(1)).union(
            day1.withColumn("batch", f.lit(2))
        )
        out = scd.slowly_changing_dimension(
            df_src=df, business_keys="id", non_copy_fields=["batch"]
        )
        assert out.count() == 2

    def test_exact_duplicate_rows_are_allowed(self, scd, day1):
        out = scd.slowly_changing_dimension(df_src=day1.union(day1), business_keys="id")
        assert sorted((r.id, r.city) for r in out.collect()) == [(1, "A"), (2, "B")]

    def test_target_missing_columns(self, scd, day1, spark_session):
        target = scd.slowly_changing_dimension(df_src=day1, business_keys="id").drop(
            "city"
        )
        day2 = spark_session.createDataFrame([(1, "A", datetime(2024, 1, 2))], SCHEMA)
        with pytest.raises(
            DataValidationError,
            match=r"Target DataFrame is missing columns: \['city'\]",
        ):
            scd.slowly_changing_dimension(
                df_src=day2, df_tgt=target, business_keys="id"
            )

    def test_target_with_multiple_active_rows_per_key(self, scd, day1, spark_session):
        target = scd.slowly_changing_dimension(df_src=day1, business_keys="id")
        target = target.union(target)
        day2 = spark_session.createDataFrame([(1, "A", datetime(2024, 1, 2))], SCHEMA)
        with pytest.raises(DataValidationError, match="more than one active record"):
            scd.slowly_changing_dimension(
                df_src=day2, df_tgt=target, business_keys="id"
            )


class TestFullHistoryTarget:
    """Passing the whole dimension (closed + active rows) as df_tgt must behave like active-only."""

    @pytest.fixture
    def history(self, spark_session):
        scd = SCD2Loader(spark_session)
        snapshots = spark_session.createDataFrame(
            [(1, "A", datetime(2024, 1, 1)), (1, "B", datetime(2024, 1, 2))], SCHEMA
        )
        return scd.slowly_changing_dimension(df_src=snapshots, business_keys="id").drop(
            "upsert_flag"
        )

    def test_history_has_closed_and_active_rows(self, history):
        assert sorted(r.active_flag for r in history.collect()) == [False, True]

    def test_unchanged_record_produces_no_operations(self, spark_session, history):
        day3 = spark_session.createDataFrame([(1, "B", datetime(2024, 1, 3))], SCHEMA)
        out = SCD2Loader(spark_session).slowly_changing_dimension(
            df_src=day3, df_tgt=history, business_keys="id"
        )
        assert out.collect() == []

    def test_changed_record_closes_only_the_active_version(
        self, spark_session, history
    ):
        day3 = spark_session.createDataFrame([(1, "C", datetime(2024, 1, 3))], SCHEMA)
        out = SCD2Loader(spark_session).slowly_changing_dimension(
            df_src=day3, df_tgt=history, business_keys="id"
        )
        rows = sorted(
            (r.city, r.valid_from, r.valid_until, r.active_flag, r.upsert_flag)
            for r in out.collect()
        )
        assert rows == [
            ("B", datetime(2024, 1, 2), datetime(2024, 1, 3), False, "U"),
            ("C", datetime(2024, 1, 3), datetime(9999, 12, 31), True, "I"),
        ]
