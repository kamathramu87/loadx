"""Delta persistence for the operation rows produced by the SCD2 transformer."""
from __future__ import annotations

from typing import TYPE_CHECKING
from uuid import uuid4

from loadx.exceptions import ConfigurationError, DataValidationError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession


def quote_identifier(name: str) -> str:
    """Quote a literal Spark identifier, including embedded backticks."""
    return "`" + name.replace("`", "``") + "`"


def quote_table(name: str) -> str:
    """Accept an unquoted table, schema.table, or catalog.schema.table name."""
    parts = name.split(".")
    if not 1 <= len(parts) <= 3 or any(not p.strip() or "`" in p for p in parts):
        raise ConfigurationError("target_table must be an unquoted table identifier")
    return ".".join(quote_identifier(p) for p in parts)


def merge_changes(
    spark: SparkSession,
    changes: DataFrame,
    target_table: str,
    business_keys: list[str],
    valid_from: str,
    target_columns: list[str],
) -> None:
    """Apply SCD2 operations atomically, retaining rows absent from the changes.

    Explicit assignments keep the source-only upsert_flag out of new targets.
    Match on the version start as well as the business key so closing a version
    cannot overwrite another version of the same dimension member.
    """
    missing = set(target_columns) - set(changes.columns)
    if missing:
        raise DataValidationError(
            f"Merge source is missing target columns: {sorted(missing)}"
        )
    view = f"loadx_scd2_{uuid4().hex}"
    condition = " AND ".join(
        f"t.{quote_identifier(c)} = s.{quote_identifier(c)}"
        for c in [*business_keys, valid_from]
    )
    assignments = ", ".join(
        f"t.{quote_identifier(c)} = s.{quote_identifier(c)}" for c in target_columns
    )
    columns = ", ".join(quote_identifier(c) for c in target_columns)
    values = ", ".join(f"s.{quote_identifier(c)}" for c in target_columns)
    changes.createOrReplaceTempView(view)
    try:
        spark.sql(
            f"MERGE INTO {quote_table(target_table)} AS t "  # noqa: S608 -- all identifiers are quoted
            f"USING {quote_identifier(view)} AS s ON {condition} "
            f"WHEN MATCHED AND s.`upsert_flag` = 'U' THEN UPDATE SET {assignments} "
            f"WHEN NOT MATCHED AND s.`upsert_flag` = 'I' "
            f"THEN INSERT ({columns}) VALUES ({values})"
        ).collect()
    finally:
        spark.catalog.dropTempView(view)
