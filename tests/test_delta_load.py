from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from loadx import SCD2Loader
from loadx.exceptions import ConfigurationError, DataValidationError
from loadx.scd2.delta import merge_changes, quote_table


def test_merge_matches_version_and_keeps_operation_flag_source_only():
    spark, changes = MagicMock(), MagicMock()
    changes.columns = ["id", "region", "start", "city", "upsert_flag"]
    merge_changes(
        spark,
        changes,
        "catalog.schema.dim",
        ["id", "region"],
        "start",
        changes.columns[:-1],
    )
    sql = spark.sql.call_args.args[0]
    assert (
        "t.`id` = s.`id` AND t.`region` = s.`region` AND t.`start` = s.`start`" in sql
    )
    assert "WHEN MATCHED AND s.`upsert_flag` = 'U'" in sql
    assert "WHEN NOT MATCHED AND s.`upsert_flag` = 'I'" in sql
    assert "t.`upsert_flag`" not in sql
    assert "DELETE" not in sql
    spark.sql.return_value.collect.assert_called_once()
    spark.catalog.dropTempView.assert_called_once_with(
        changes.createOrReplaceTempView.call_args.args[0]
    )


def test_merge_cleans_up_view_on_failure():
    spark, changes = MagicMock(), MagicMock()
    changes.columns = ["id", "start", "upsert_flag"]
    spark.sql.side_effect = RuntimeError("merge failed")
    with pytest.raises(RuntimeError, match="merge failed"):
        merge_changes(spark, changes, "dim", ["id"], "start", ["id", "start"])
    spark.catalog.dropTempView.assert_called_once()


def test_missing_target_columns_fail_before_merge():
    spark, changes = MagicMock(), MagicMock()
    changes.columns = ["id"]
    with pytest.raises(DataValidationError, match="missing target columns"):
        merge_changes(spark, changes, "dim", ["id"], "start", ["id", "start"])
    spark.sql.assert_not_called()


@pytest.mark.parametrize("name", ["", "a..b", "a.b.c.d", "`a`.b"])
def test_invalid_table_identifier(name):
    with pytest.raises(ConfigurationError):
        quote_table(name)


def test_identifier_quoting():
    assert quote_table("my-catalog.my-schema.dim") == "`my-catalog`.`my-schema`.`dim`"


def test_initial_load_does_not_overwrite_existing_table():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = False
    loader = SCD2Loader(spark)
    loader._process = MagicMock()
    changes = loader._process.return_value
    result = loader.load(MagicMock(), "catalog.schema.dim", "id")
    changes.drop.assert_called_once_with("upsert_flag")
    writer = changes.drop.return_value.write
    writer.format.assert_called_once_with("delta")
    writer.format.return_value.mode.assert_called_once_with("error")
    writer.format.return_value.mode.return_value.saveAsTable.assert_called_once_with(
        "`catalog`.`schema`.`dim`"
    )
    assert result is spark.table.return_value


def test_existing_load_uses_full_target_and_custom_version_key():
    from unittest.mock import patch

    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    target = spark.table.return_value
    target.columns = ["id", "start", "end"]
    loader = SCD2Loader(spark)
    loader._process = MagicMock()
    loader._process.return_value.columns = [
        *target.columns,
        "snapshot_date",
        "upsert_flag",
    ]
    with patch("loadx.scd2.loader.merge_changes") as merge:
        source = MagicMock()
        loader.load(
            source,
            "dim",
            "id",
            scd_columns={"valid_from": "start", "valid_until": "end"},
        )
    assert loader._process.call_args.args[:2] == (source, target)
    merge.assert_called_once_with(
        spark, loader._process.return_value, "dim", ["id"], "start", target.columns
    )


def test_load_rejects_missing_output_columns_even_for_empty_target():
    spark = MagicMock()
    spark.catalog.tableExists.return_value = True
    spark.table.return_value.columns = ["id", "valid_from", "valid_until"]
    loader = SCD2Loader(spark)
    loader._process = MagicMock()
    loader._process.return_value.columns = [
        "id",
        "valid_from",
        "valid_until",
        "row_hash",
        "upsert_flag",
    ]
    with pytest.raises(DataValidationError, match="missing SCD2 output columns"):
        loader.load(MagicMock(), "dim", "id")
    spark.sql.assert_not_called()
