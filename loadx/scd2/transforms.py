from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from loadx.exceptions import (
    BusinessKeysEmptyError,
    EmptyDataExceptionError,
    OldDataExceptionError,
)
from loadx.scd2.config import (
    COL_DATE_LEAD,
    COL_DELETE_FLAG,
    COL_DELETED,
    COL_NEXT_CHANGE,
    COL_ORIG_VALID_FROM,
    COL_ORIG_VALID_UNTIL,
    COL_ROW_HASH_CHANGED,
    COL_ROW_HASH_CHANGED_LAG,
    HASH_SEPARATOR,
    UPSERT_FLAG_COLUMN,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.window import WindowSpec

    from loadx.scd2.config import SCD2Config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


def validate_config(config: SCD2Config) -> None:
    if not config.business_keys:
        raise BusinessKeysEmptyError
    if not config.date_column:
        raise ValueError("date_column cannot be empty")


def validate_inputs(
    df_src: DataFrame, business_keys: list[str], date_column: str
) -> None:
    if df_src.isEmpty():
        raise EmptyDataExceptionError
    missing = [
        col for col in [*business_keys, date_column] if col not in df_src.columns
    ]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def validate_data_freshness(
    df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
) -> None:
    tgt_max = _get_max_date(df_tgt, config.scd_columns.valid_from)
    src_max = _get_max_date(df_src, config.date_column)
    if src_max < tgt_max:
        logger.error(
            "Source data (%s) is older than target data (%s)", src_max, tgt_max
        )
        raise OldDataExceptionError


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _get_max_date(df: DataFrame, date_column: str) -> Any:
    row = df.agg(f.max(date_column).alias("max_date")).first()
    return row["max_date"] if row else ""


# ---------------------------------------------------------------------------
# Transformations
# ---------------------------------------------------------------------------


def prepare_source_data(df_src: DataFrame, config: SCD2Config) -> DataFrame:
    df = df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
    return df.withColumn(COL_ORIG_VALID_FROM, f.lit(None).cast("timestamp")).withColumn(
        COL_ORIG_VALID_UNTIL, f.lit(None).cast("timestamp")
    )


def handle_incremental_load(
    df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
) -> DataFrame:
    logger.info("Processing incremental load")
    validate_data_freshness(df_src, df_tgt, config)

    tgt_max_date = _get_max_date(df_tgt, config.scd_columns.valid_from)

    df_tgt_base = (
        df_tgt.withColumnRenamed(config.scd_columns.valid_from, COL_ORIG_VALID_FROM)
        .withColumnRenamed(config.scd_columns.valid_until, COL_ORIG_VALID_UNTIL)
        .drop(*config.scd_columns.field_list())
        .withColumn(config.date_column, f.lit(tgt_max_date))
    )

    return df_src.filter(f.col(config.date_column) > f.lit(tgt_max_date)).union(
        df_tgt_base.select(df_src.columns)
    )


def apply_hash_columns(
    df: DataFrame, config: SCD2Config, source_columns: list[str]
) -> DataFrame:
    hashable = [c for c in source_columns if c not in (config.ignore_columns or [])]
    return df.withColumn(
        COL_ROW_HASH_CHANGED,
        f.sha2(f.concat_ws(HASH_SEPARATOR, *hashable, COL_DELETED), 256),
    ).withColumn(
        config.scd_columns.row_hash,
        f.sha2(f.concat_ws(HASH_SEPARATOR, *hashable), 256),
    )


def filter_for_changes(
    df: DataFrame, config: SCD2Config, window: WindowSpec
) -> DataFrame:
    return (
        df.withColumn(
            COL_ROW_HASH_CHANGED_LAG, f.lag(COL_ROW_HASH_CHANGED).over(window)
        )
        .filter(
            f.col(COL_ROW_HASH_CHANGED_LAG).isNull()
            | (f.col(COL_ROW_HASH_CHANGED_LAG) != f.col(COL_ROW_HASH_CHANGED))
        )
        .drop(COL_ROW_HASH_CHANGED_LAG, COL_ROW_HASH_CHANGED)
        .withColumn(COL_NEXT_CHANGE, f.lead(config.date_column).over(window))
        .withColumn(COL_DELETE_FLAG, f.lead(COL_DELETED).over(window))
    )


def process_deletions(df: DataFrame, config: SCD2Config) -> DataFrame:
    date_col = config.date_column
    date_col_r = f"{date_col}_r"

    snapshot_dates = (
        df.select(date_col)
        .distinct()
        .withColumn(
            "next_date_available",
            f.lead(date_col).over(Window.orderBy(date_col)),
        )
        .withColumnRenamed(date_col, date_col_r)
    )

    max_row = snapshot_dates.agg(f.max(date_col_r).alias("max_date")).first()
    max_snapshot_date = max_row.max_date if max_row else None

    bk_window = Window.partitionBy(config.business_keys).orderBy(date_col)

    df_flagged = (
        df.join(snapshot_dates, f.col(date_col) == f.col(date_col_r), "left")
        .withColumn(COL_DATE_LEAD, f.lead(date_col).over(bk_window))
        .withColumn(
            COL_DELETED,
            f.when(
                (f.col("next_date_available") != f.col(COL_DATE_LEAD))
                | (f.col(COL_DATE_LEAD).isNull() & (df[date_col] != max_snapshot_date)),
                True,
            ).otherwise(False),
        )
    )

    base_cols = df_flagged.drop(
        "next_date_available", COL_DATE_LEAD, date_col_r
    ).columns
    return (
        df_flagged.drop("next_date_available", COL_DATE_LEAD, date_col_r)
        .withColumn(COL_DELETED, f.lit(False))
        .union(
            df_flagged.where(f.col(COL_DELETED))
            .drop(date_col, COL_DATE_LEAD, date_col_r)
            .withColumnRenamed("next_date_available", date_col)
            .select(base_cols)
        )
    )


def add_support_columns(df: DataFrame, config: SCD2Config) -> DataFrame:
    result = (
        df.filter(~f.col(COL_DELETED))
        .withColumn(
            config.scd_columns.valid_from,
            f.coalesce(
                df[COL_ORIG_VALID_FROM], f.col(config.date_column).cast("timestamp")
            ),
        )
        .withColumn(
            config.date_column,
            f.coalesce(df[COL_ORIG_VALID_FROM], f.col(config.date_column)),
        )
        .withColumn(
            config.scd_columns.valid_until,
            f.coalesce(df[COL_NEXT_CHANGE], f.lit(config.open_end_date)),
        )
        .withColumn(
            config.scd_columns.active_flag,
            f.when(
                f.col(config.scd_columns.valid_until).isNull()
                | (f.col(config.scd_columns.valid_until) == config.open_end_date),
                True,
            ).otherwise(False),
        )
        .withColumn(
            UPSERT_FLAG_COLUMN,
            f.when(df[COL_ORIG_VALID_FROM].isNull(), "I").otherwise("U"),
        )
        .withColumn(config.scd_columns.insert_date, f.current_timestamp())
    )

    if config.source_type == "full":
        result = result.withColumn(
            config.scd_columns.delete_flag,
            f.coalesce(result[COL_DELETE_FLAG], f.lit(False)),
        )

    if config.enable_latest_record_flag:
        latest_window = Window.partitionBy(config.business_keys).orderBy(
            f.col(config.scd_columns.valid_from).desc()
        )
        result = (
            result.withColumn("_row_num", f.row_number().over(latest_window))
            .withColumn(
                config.scd_columns.latest_record_flag,
                f.when(f.col("_row_num") == 1, True).otherwise(False),
            )
            .drop("_row_num")
        )

    return result


def finalize_output(df: DataFrame, config: SCD2Config) -> DataFrame:
    internal_cols = {
        COL_ORIG_VALID_FROM,
        COL_ORIG_VALID_UNTIL,
        COL_NEXT_CHANGE,
        COL_DELETED,
        COL_DELETE_FLAG,
        UPSERT_FLAG_COLUMN,
        *config.scd_columns.column_list(),
    }
    source_columns = [c for c in df.columns if c not in internal_cols]

    scd_output = [
        c
        for c in config.scd_columns.column_list()
        if (
            config.enable_latest_record_flag
            or c != config.scd_columns.latest_record_flag
        )
        and (config.source_type == "full" or c != config.scd_columns.delete_flag)
    ]

    return df.filter(
        (
            f.coalesce(df[COL_ORIG_VALID_UNTIL], f.lit(config.open_end_date))
            != df[config.scd_columns.valid_until]
        )
        | df[COL_ORIG_VALID_FROM].isNull()
    ).select(source_columns + scd_output + [UPSERT_FLAG_COLUMN])
