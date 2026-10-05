from __future__ import annotations

import pytest
from datetime import datetime
from unittest.mock import MagicMock

from loadx.scd2 import transforms
from loadx.scd2.config import SCD2Config, SCD2ColumnNames
from loadx.exceptions import (
    BusinessKeysEmptyError,
    ConfigurationError,
    DataValidationError,
    EmptyDataExceptionError,
)
from loadx.utils.spark_factory import SparkSessionFactory


class TestSCD2Config:
    """Test cases for SCD2Config class."""

    def test_config_creation_with_defaults(self):
        """Test creating config with default values."""
        config = SCD2Config.create(business_keys=["id"])

        assert config.business_keys == ["id"]
        assert config.date_column == "snapshot_date"
        assert config.ignore_columns == []
        assert config.non_copy_fields == []
        assert config.open_end_date == datetime(9999, 12, 31)
        assert isinstance(config.scd_columns, SCD2ColumnNames)

    def test_config_creation_with_string_business_key(self):
        """Test creating config with string business key."""
        config = SCD2Config.create(business_keys="id")

        assert config.business_keys == ["id"]

    def test_config_creation_with_custom_values(self):
        """Test creating config with custom values."""
        custom_date = datetime(2023, 12, 31)
        config = SCD2Config.create(
            business_keys=["id", "type"],
            date_column="process_date",
            ignore_columns=["created_at"],
            non_copy_fields=["temp_field"],
            open_end_date=custom_date,
        )

        assert config.business_keys == ["id", "type"]
        assert config.date_column == "process_date"
        assert config.ignore_columns == ["created_at"]
        assert config.non_copy_fields == ["temp_field"]
        assert config.open_end_date == custom_date


class TestSCD2ColumnNames:
    """Test cases for SCD2ColumnNames class."""

    def test_default_column_names(self):
        """Test default SCD2 column names."""
        columns = SCD2ColumnNames()

        assert columns.valid_from == "valid_from"
        assert columns.valid_until == "valid_until"
        assert columns.active_flag == "active_flag"
        assert columns.delete_flag == "delete_flag"
        assert columns.row_hash == "row_hash"

    def test_from_dict_creation(self):
        """Test creating SCD2ColumnNames from dictionary."""
        data = {
            "valid_from": "start_date",
            "valid_until": "end_date",
            "invalid_field": "should_be_ignored",
        }

        columns = SCD2ColumnNames.from_dict(data)

        assert columns.valid_from == "start_date"
        assert columns.valid_until == "end_date"
        assert columns.active_flag == "active_flag"  # default value
        assert not hasattr(columns, "invalid_field")

    def test_field_list(self):
        """Test getting field list."""
        columns = SCD2ColumnNames()
        field_list = columns.field_list()

        expected_fields = [
            "valid_from",
            "valid_until",
            "active_flag",
            "delete_flag",
            "row_hash",
            "insert_date",
            "latest_record_flag",
        ]
        assert set(field_list) == set(expected_fields)


class TestValidation:
    """Test cases for SCD2Validator class."""

    def test_validate_inputs_empty_business_keys(self):
        """Test that validate_inputs reports missing columns when business keys are empty."""
        df_mock = MagicMock()
        df_mock.isEmpty.return_value = False
        df_mock.columns = ["id", "name", "snapshot_date"]

        # empty business_keys means no column check is performed — should not raise
        transforms.validate_inputs(df_mock, [], "snapshot_date")

    def test_validate_inputs_empty_date_column(self):
        """Test validation raises when date column is missing from DataFrame."""
        df_mock = MagicMock()
        df_mock.isEmpty.return_value = False
        df_mock.columns = ["id", "name", "snapshot_date"]

        with pytest.raises(ValueError, match="Missing required columns"):
            transforms.validate_inputs(df_mock, ["id"], "")

    def test_validate_inputs_empty_dataframe(self):
        """Test validation with empty DataFrame."""
        df_mock = MagicMock()
        df_mock.isEmpty.return_value = True

        with pytest.raises(EmptyDataExceptionError):
            transforms.validate_inputs(df_mock, ["id"], "snapshot_date")

    def test_validate_inputs_missing_columns(self):
        """Test validation with missing required columns."""
        df_mock = MagicMock()
        df_mock.isEmpty.return_value = False
        df_mock.columns = ["id", "name"]  # missing snapshot_date

        with pytest.raises(ValueError, match="Missing required columns"):
            transforms.validate_inputs(df_mock, ["id"], "snapshot_date")

    def test_validate_inputs_success(self):
        """Test successful validation."""
        df_mock = MagicMock()
        df_mock.isEmpty.return_value = False
        df_mock.columns = ["id", "name", "snapshot_date"]

        # Should not raise any exception
        transforms.validate_inputs(df_mock, ["id"], "snapshot_date")

    def test_validate_config_empty_business_keys(self):
        """Test config validation with empty business keys."""
        config = MagicMock()
        config.business_keys = []

        with pytest.raises(BusinessKeysEmptyError):
            transforms.validate_config(config)

    def test_validate_config_empty_date_column(self):
        """Test config validation with empty date column."""
        config = MagicMock()
        config.business_keys = ["id"]
        config.date_column = ""

        with pytest.raises(ValueError, match="date_column cannot be empty"):
            transforms.validate_config(config)

    def test_validate_config_valid(self):
        """Test config validation passes with valid config."""
        config = SCD2Config.create(business_keys=["id"], date_column="snapshot_date")

        # Should not raise
        transforms.validate_config(config)

    def test_validate_config_invalid_source_type(self):
        """A misspelled source_type must not silently skip deletion detection."""
        config = SCD2Config.create(business_keys=["id"], source_type="typo")  # type: ignore[arg-type]

        with pytest.raises(ConfigurationError, match="source_type must be one of"):
            transforms.validate_config(config)

    def test_validate_config_accepts_source_type_string(self):
        """Plain string values matching a SourceType are accepted."""
        transforms.validate_config(SCD2Config.create(business_keys=["id"], source_type="incremental"))  # type: ignore[arg-type]

    def test_validate_config_date_column_is_business_key(self):
        config = SCD2Config.create(business_keys=["id", "snapshot_date"])

        with pytest.raises(ConfigurationError, match="cannot also be a business key"):
            transforms.validate_config(config)

    def test_validate_config_non_copy_fields_drops_key(self):
        config = SCD2Config.create(business_keys=["id"], non_copy_fields=["id"])

        with pytest.raises(ConfigurationError, match="non_copy_fields cannot include"):
            transforms.validate_config(config)

    def test_validate_config_duplicate_output_names(self):
        config = SCD2Config.create(
            business_keys=["id"], scd_columns={"valid_from": "x", "valid_until": "x"}
        )

        with pytest.raises(
            ConfigurationError, match=r"Duplicate output column names: \['x'\]"
        ):
            transforms.validate_config(config)

    def test_validate_config_output_name_is_upsert_flag(self):
        config = SCD2Config.create(
            business_keys=["id"], scd_columns={"row_hash": "upsert_flag"}
        )

        with pytest.raises(ConfigurationError, match="Duplicate output column names"):
            transforms.validate_config(config)

    def test_validate_config_output_name_collides_with_internal(self):
        config = SCD2Config.create(
            business_keys=["id"], scd_columns={"row_hash": "deleted"}
        )

        with pytest.raises(ConfigurationError, match="collide with internal columns"):
            transforms.validate_config(config)

    def test_validate_config_row_hash_renamed_to_internal_delete_flag(self):
        """'delete_flag' is only allowed as the name of the delete_flag output itself."""
        config = SCD2Config.create(
            business_keys=["id"],
            scd_columns={"delete_flag": "is_deleted", "row_hash": "delete_flag"},
        )

        with pytest.raises(ConfigurationError, match="collide with internal columns"):
            transforms.validate_config(config)

    def test_validation_errors_are_value_errors(self):
        """Callers catching ValueError keep working."""
        assert issubclass(ConfigurationError, ValueError)
        assert issubclass(DataValidationError, ValueError)
        assert ConfigurationError("x").message == "Configuration error: x"


class TestSparkSessionFactory:
    """Test cases for SparkSessionFactory class."""

    def test_get_or_create_session_with_existing(self):
        """Test getting existing session."""
        existing_session = MagicMock()

        result = SparkSessionFactory.get_or_create_session(existing_session)

        assert result == existing_session

    def test_get_or_create_session_without_existing(self):
        """Test creating new session when none provided."""
        # In this environment Spark is available, so just test it works
        session = SparkSessionFactory.get_or_create_session(None)
        assert session is not None
        assert hasattr(session, "createDataFrame")
