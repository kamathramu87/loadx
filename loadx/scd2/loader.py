from __future__ import annotations

import logging
from typing import TYPE_CHECKING

import pyspark.sql.functions as f
from pyspark.sql.window import Window

from loadx.scd2 import transforms as t
from loadx.scd2.config import (
    COL_DELETED,
    DEFAULT_DATE_COLUMN,
    OPEN_END_DATE,
    SCD2Config,
    SourceType,
)
from loadx.utils.spark_factory import SparkSessionFactory

if TYPE_CHECKING:
    from datetime import datetime

    from pyspark.sql import DataFrame, SparkSession

    from loadx.scd2.config import SCD2ColumnNames

logger = logging.getLogger(__name__)


class SCD2Loader:
    """Slowly Changing Dimension Type 2 loader for PySpark DataFrames.

    Tracks historical changes using valid_from/valid_until date ranges,
    hash-based change detection, and active/delete flags.
    """

    def __init__(self, spark_session: SparkSession | None = None) -> None:
        self.spark = SparkSessionFactory.get_or_create_session(spark_session)

    def slowly_changing_dimension(
        self,
        df_src: DataFrame,
        business_keys: list[str] | str,
        date_column: str = DEFAULT_DATE_COLUMN,
        df_tgt: DataFrame | None = None,
        ignore_columns: list[str] | None = None,
        non_copy_fields: list[str] | None = None,
        open_end_date: datetime | None = OPEN_END_DATE,
        scd_columns: SCD2ColumnNames | dict[str, str] | None = None,
        enable_latest_record_flag: bool = False,
        source_type: SourceType = SourceType.FULL,
    ) -> DataFrame:
        """Process slowly changing dimension type 2 transformation.

        Args:
            df_src: Source DataFrame containing the data to process
            business_keys: List of columns that constitute the business key
            date_column: Column name containing snapshot dates
            df_tgt: Optional target DataFrame for incremental loads
            ignore_columns: Columns to ignore when calculating row hashes
            non_copy_fields: Fields to exclude from source to target
            open_end_date: Date to use for active records (default: 9999-12-31)
            scd_columns: Override default SCD2 output column names. Accepts an
                `SCD2ColumnNames` instance (recommended, full IDE type hints) or a plain
                dict with any subset of keys: `valid_from`, `valid_until`,
                `active_flag`, `delete_flag`, `row_hash`, `insert_date`.
            enable_latest_record_flag: When `True`, adds a `latest_record_flag` column
                that is `True` for the most recent record per business key.
            source_type: ``"full"`` (default) for full snapshot sources where
                deletions are detected between snapshots and a ``delete_flag``
                column is included in the output. ``"incremental"`` for feeds
                that only contain changed/new records — deletion detection is
                skipped and ``delete_flag`` is omitted from the output.

        Returns:
            DataFrame with SCD2 columns and transformations applied

        Raises:
            EmptyDataExceptionError: When source DataFrame is empty
            OldDataExceptionError: When source data is older than target data
            ValueError: When invalid parameters are provided
        """
        config = SCD2Config.create(
            business_keys=business_keys,
            date_column=date_column,
            ignore_columns=ignore_columns,
            non_copy_fields=non_copy_fields,
            open_end_date=open_end_date,
            scd_columns=scd_columns,
            enable_latest_record_flag=enable_latest_record_flag,
            source_type=source_type,
        )
        return self._process(df_src, df_tgt, config)

    def _process(
        self, df_src: DataFrame, df_tgt: DataFrame | None, config: SCD2Config
    ) -> DataFrame:
        t.validate_config(config)
        t.validate_inputs(df_src, config.business_keys, config.date_column)

        source_columns = self._source_columns(df_src, config)

        df = t.prepare_source_data(df_src, config)

        if df_tgt and not df_tgt.isEmpty():
            df = t.handle_incremental_load(df, df_tgt, config)

        window = Window.partitionBy(config.business_keys).orderBy(config.date_column)

        df = df.distinct()
        df = (
            t.process_deletions(df, config)
            if config.source_type == "full"
            else df.withColumn(COL_DELETED, f.lit(False))
        )
        df = t.apply_hash_columns(df, config, source_columns)
        df = t.filter_for_changes(df, config, window)
        df = t.add_support_columns(df, config)
        return t.finalize_output(df, config)

    @staticmethod
    def _source_columns(df_src: DataFrame, config: SCD2Config) -> list[str]:
        df = df_src.drop(*config.non_copy_fields) if config.non_copy_fields else df_src
        cols = df.columns.copy()
        if config.date_column in cols:
            cols.remove(config.date_column)
        return cols
