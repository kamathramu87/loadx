from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from loadx.exceptions import (
    BusinessKeysEmptyError,
    ConfigurationError,
    DataValidationError,
    EmptyDataExceptionError,
    OldDataExceptionError,
)
from loadx.scd2.config import (
    COL_DATE_LEAD,
    COL_DELETE_FLAG,
    COL_DELETED,
    COL_NEXT_CHANGE,
    COL_NEXT_DATE_AVAILABLE,
    COL_ORIG_VALID_FROM,
    COL_ORIG_VALID_UNTIL,
    COL_ROW_HASH_CHANGED,
    COL_ROW_HASH_CHANGED_LAG,
    COL_ROW_NUM,
    SNAPSHOT_DATE_SUFFIX,
    UPSERT_FLAG_COLUMN,
    SourceType,
    internal_columns,
)

if TYPE_CHECKING:
    from pyspark.sql import DataFrame
    from pyspark.sql.window import WindowSpec

    from loadx.scd2.config import SCD2Config

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------


MAX_REPORTED_CONFLICTS = 5


def validate_config(config: SCD2Config) -> None:
    if not config.business_keys:
        raise BusinessKeysEmptyError
    if not config.date_column:
        raise ConfigurationError("date_column cannot be empty")
    try:
        SourceType(config.source_type)
    except ValueError:
        valid = [s.value for s in SourceType]
        raise ConfigurationError(
            f"source_type must be one of {valid}, got {config.source_type!r}"
        ) from None
    if config.date_column in config.business_keys:
        raise ConfigurationError(
            f"date_column {config.date_column!r} cannot also be a business key"
        )
    dropped_keys = [
        c
        for c in [*config.business_keys, config.date_column]
        if c in (config.non_copy_fields or [])
    ]
    if dropped_keys:
        raise ConfigurationError(
            f"non_copy_fields cannot include business keys or date_column: {dropped_keys}"
        )
    _validate_output_column_names(config)


def _validate_output_column_names(config: SCD2Config) -> None:
    names = [*config.scd_columns.column_list(), UPSERT_FLAG_COLUMN]
    duplicates = sorted({n for n in names if names.count(n) > 1})
    if duplicates:
        raise ConfigurationError(f"Duplicate output column names: {duplicates}")

    # The default delete_flag output name is shared with the internal column that feeds it.
    reserved = internal_columns(config.date_column)
    if config.scd_columns.delete_flag == COL_DELETE_FLAG:
        reserved = reserved - {COL_DELETE_FLAG}
    collisions = sorted(set(names) & reserved)
    if collisions:
        raise ConfigurationError(
            f"Output column names collide with internal columns: {collisions}"
        )


def validate_inputs(
    df_src: DataFrame, business_keys: list[str], date_column: str
) -> None:
    if df_src.isEmpty():
        raise EmptyDataExceptionError
    missing = [
        col for col in [*business_keys, date_column] if col not in df_src.columns
    ]
    if missing:
        raise DataValidationError(f"Missing required columns: {missing}")


def validate_source_columns(df_src: DataFrame, config: SCD2Config) -> None:
    for param, cols in (
        ("ignore_columns", config.ignore_columns),
        ("non_copy_fields", config.non_copy_fields),
    ):
        unknown = [c for c in cols or [] if c not in df_src.columns]
        if unknown:
            raise DataValidationError(
                f"{param} not found in source DataFrame: {unknown}"
            )

    # Any source column with one of these names would be overwritten or dropped silently.
    reserved = (
        internal_columns(config.date_column)
        | set(config.scd_columns.column_list())
        | {UPSERT_FLAG_COLUMN}
    )
    kept = set(df_src.columns) - set(config.non_copy_fields or [])
    collisions = sorted(kept & reserved)
    if collisions:
        raise DataValidationError(
            f"Source columns use names reserved for SCD2 output or processing: {collisions}. "
            "Rename them, list them in non_copy_fields, or rename the SCD2 output "
            "columns via scd_columns."
        )


def validate_source_rows(df_src: DataFrame, config: SCD2Config) -> None:
    """Reject null keys/dates and rows that conflict on business key + snapshot date.

    Exact duplicate rows are allowed; they are collapsed later in the pipeline.
    """
    keys = [*config.business_keys, config.date_column]
    df = df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
    has_null = f.greatest(*[f.col(c).isNull() for c in keys], f.lit(False))
    count_column = _temporary_column(keys, "_validation_count")
    groups = df.distinct().groupBy(*keys).agg(f.count(f.lit(1)).alias(count_column))
    stats = groups.agg(
        f.sum(f.when(has_null, f.col(count_column)).otherwise(0)).alias("null_rows"),
        f.sum(f.when(~has_null & (f.col(count_column) > 1), 1).otherwise(0)).alias(
            "conflicts"
        ),
    ).first()
    if stats is None:
        return

    if stats["null_rows"]:
        raise DataValidationError(
            f"{stats['null_rows']} source row(s) have a null value in business keys "
            f"or date_column {keys}"
        )
    if stats["conflicts"]:
        sample = [
            r.asDict()
            for r in groups.filter(f.col(count_column) > 1)
            .select(*keys)
            .limit(MAX_REPORTED_CONFLICTS)
            .collect()
        ]
        raise DataValidationError(
            f"{stats['conflicts']} business key + {config.date_column} combination(s) "
            f"have conflicting rows with different values, e.g. {sample}. Deduplicate "
            "the source so each key has at most one row per snapshot."
        )


def validate_target(
    df_tgt: DataFrame, config: SCD2Config, source_columns: list[str]
) -> None:
    required = [
        *source_columns,
        config.scd_columns.valid_from,
        config.scd_columns.valid_until,
    ]
    if config.enable_latest_record_flag:
        required = list(dict.fromkeys([*required, *output_scd_columns(config)]))
    missing = [c for c in required if c not in df_tgt.columns]
    if missing:
        raise DataValidationError(f"Target DataFrame is missing columns: {missing}")

    count_column = _temporary_column(config.business_keys, "_validation_count")
    duplicated = (
        active_target_rows(df_tgt, config)
        .groupBy(*config.business_keys)
        .agg(f.count(f.lit(1)).alias(count_column))
        .filter(f.col(count_column) > 1)
        .select(*config.business_keys)
        .limit(MAX_REPORTED_CONFLICTS)
        .collect()
    )
    if duplicated:
        raise DataValidationError(
            "Target has more than one active record for business key(s) "
            f"{[r.asDict() for r in duplicated]}"
        )


def validate_data_freshness(
    df_src: DataFrame, df_tgt: DataFrame, config: SCD2Config
) -> Any:
    tgt_max = _get_max_date(df_tgt, config.scd_columns.valid_from)
    src_max = _get_max_date(df_src, config.date_column)
    if src_max < tgt_max:
        logger.error(
            "Source data (%s) is older than target data (%s)", src_max, tgt_max
        )
        raise OldDataExceptionError
    return tgt_max


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def _get_max_date(df: DataFrame, date_column: str) -> Any:
    row = df.agg(f.max(date_column).alias("max_date")).first()
    return row["max_date"] if row else ""


def _temporary_column(columns: list[str], prefix: str) -> str:
    """Choose an aggregation alias without shadowing a grouping column."""
    existing = {c.lower() for c in columns}
    while prefix.lower() in existing:
        prefix += "_"
    return prefix


def output_scd_columns(config: SCD2Config) -> list[str]:
    """SCD2 columns emitted for this config, honouring optional flags and source type."""
    return [
        c
        for c in config.scd_columns.column_list()
        if (
            config.enable_latest_record_flag
            or c != config.scd_columns.latest_record_flag
        )
        and (config.source_type == "full" or c != config.scd_columns.delete_flag)
    ]


def active_target_rows(df_tgt: DataFrame, config: SCD2Config) -> DataFrame:
    return df_tgt.filter(
        f.col(config.scd_columns.valid_until).eqNullSafe(
            f.lit(config.open_end_date).cast("timestamp")
        )
    )


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
    tgt_max_date = validate_data_freshness(df_src, df_tgt, config)

    # Closed history rows never change; merging them would tie on tgt_max_date
    # in the date-ordered windows and corrupt the active version.
    df_tgt_base = (
        active_target_rows(df_tgt, config)
        .withColumnRenamed(config.scd_columns.valid_from, COL_ORIG_VALID_FROM)
        .withColumnRenamed(config.scd_columns.valid_until, COL_ORIG_VALID_UNTIL)
        .drop(*config.scd_columns.column_list())
        .withColumn(config.date_column, f.lit(tgt_max_date))
    )

    return df_src.filter(f.col(config.date_column) > f.lit(tgt_max_date)).union(
        df_tgt_base.select(df_src.columns)
    )


def apply_hash_columns(
    df: DataFrame, config: SCD2Config, source_columns: list[str]
) -> DataFrame:
    excluded = {*config.business_keys, *(config.ignore_columns or [])}
    hashable = sorted(c for c in source_columns if c not in excluded)
    # JSON preserves field boundaries, types and null positions; sorting makes
    # the hash independent of the source DataFrame's column order.
    payload = f.to_json(
        f.struct(*[f.col(c) for c in hashable]),
        options={
            "ignoreNullFields": "false",
            "timeZone": "UTC",
            "timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
            "timestampNTZFormat": "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
        },
    )
    return df.withColumn(
        config.scd_columns.row_hash,
        f.sha2(payload, 256),
    ).withColumn(
        COL_ROW_HASH_CHANGED,
        f.sha2(
            f.to_json(
                f.struct(
                    f.col(config.scd_columns.row_hash).alias("row_hash"),
                    f.col(COL_DELETED).alias("deleted"),
                )
            ),
            256,
        ),
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
    date_col_r = f"{date_col}{SNAPSHOT_DATE_SUFFIX}"

    snapshot_dates = (
        df.select(date_col)
        .distinct()
        .withColumn(
            COL_NEXT_DATE_AVAILABLE,
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
                (f.col(COL_NEXT_DATE_AVAILABLE) != f.col(COL_DATE_LEAD))
                | (f.col(COL_DATE_LEAD).isNull() & (df[date_col] != max_snapshot_date)),
                True,
            ).otherwise(False),
        )
    )

    base_cols = df_flagged.drop(
        COL_NEXT_DATE_AVAILABLE, COL_DATE_LEAD, date_col_r
    ).columns
    return (
        df_flagged.drop(COL_NEXT_DATE_AVAILABLE, COL_DATE_LEAD, date_col_r)
        .withColumn(COL_DELETED, f.lit(False))
        .union(
            df_flagged.where(f.col(COL_DELETED))
            .drop(date_col, COL_DATE_LEAD, date_col_r)
            .withColumnRenamed(COL_NEXT_DATE_AVAILABLE, date_col)
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
            result.withColumn(COL_ROW_NUM, f.row_number().over(latest_window))
            .withColumn(
                config.scd_columns.latest_record_flag,
                f.when(f.col(COL_ROW_NUM) == 1, True).otherwise(False),
            )
            .drop(COL_ROW_NUM)
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

    scd_output = output_scd_columns(config)

    # Null-safe: with open_end_date=None both sides can be null, and a plain !=
    # would evaluate to null and drop the update closing an active version.
    valid_until_changed = ~f.coalesce(
        df[COL_ORIG_VALID_UNTIL], f.lit(config.open_end_date)
    ).eqNullSafe(df[config.scd_columns.valid_until])

    return df.filter(valid_until_changed | df[COL_ORIG_VALID_FROM].isNull()).select(
        source_columns + scd_output + [UPSERT_FLAG_COLUMN]
    )


def clear_previous_latest_flags(
    changes: DataFrame, df_tgt: DataFrame, config: SCD2Config
) -> DataFrame:
    """Clear the latest flag on closed versions of keys that reappear.

    Preserve the historical row's dates, attributes and audit metadata. Active
    versions already receive their flag update through the normal pipeline.
    """
    columns = config.scd_columns
    inserted_keys = changes.filter(f.col(UPSERT_FLAG_COLUMN) == "I").select(
        *config.business_keys
    )
    closed_latest = df_tgt.filter(
        f.col(columns.latest_record_flag)
        & ~f.col(columns.valid_until).eqNullSafe(
            f.lit(config.open_end_date).cast("timestamp")
        )
    ).join(inserted_keys, on=config.business_keys, how="left_semi")
    if config.date_column not in closed_latest.columns:
        closed_latest = closed_latest.withColumn(
            config.date_column, f.col(columns.valid_from)
        )
    updates = closed_latest.withColumn(
        columns.latest_record_flag, f.lit(False)
    ).withColumn(UPSERT_FLAG_COLUMN, f.lit("U"))
    return changes.unionByName(updates.select(changes.columns))
