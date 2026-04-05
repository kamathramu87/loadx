from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime
from enum import StrEnum
from typing import Any


class SourceType(StrEnum):
    FULL = "full"
    INCREMENTAL = "incremental"


# Constants
OPEN_END_DATE = datetime(9999, 12, 31)
DEFAULT_DATE_COLUMN = "snapshot_date"
UPSERT_FLAG_COLUMN = "upsert_flag"
HASH_ALGORITHM = "SHA-256"
HASH_SEPARATOR = "|"

# Internal processing column names
COL_DELETED = "deleted"
COL_ORIG_VALID_FROM = "orig_valid_from"
COL_ORIG_VALID_UNTIL = "orig_valid_until"
COL_NEXT_CHANGE = "next_change"
COL_DATE_LEAD = "date_lead"
COL_ROW_HASH_CHANGED = "row_hash_changed"
COL_ROW_HASH_CHANGED_LAG = "row_hash_changed_lag"
COL_DELETE_FLAG = "delete_flag"


@dataclass
class SCD2ColumnNames:
    """Custom names for SCD2 output columns.

    All fields are optional and default to their standard names. Pass an instance
    of this class to `SCD2Loader.slowly_changing_dimension()` via `scd_columns`
    to rename any subset of output columns.

    Attributes:
        valid_from: Date when the record became active.
        valid_until: Date when the record was superseded. `9999-12-31` for currently active records.
        active_flag: `True` for the currently active version of a record.
        delete_flag: `True` if the record was deleted in the source.
        row_hash: SHA-256 hash of non-key columns, used for change detection.
        insert_date: Timestamp when this record version was written to the target table.
        latest_record_flag: `True` for the most recent record per business key.
            Only present when `enable_latest_record_flag=True`.

    Example:
        ```python
        from loadx import SCD2ColumnNames

        SCD2ColumnNames(valid_from="eff_start_date", valid_until="eff_end_date")
        ```
    """

    valid_from: str = "valid_from"
    valid_until: str = "valid_until"
    active_flag: str = "active_flag"
    delete_flag: str = "delete_flag"
    row_hash: str = "row_hash"
    insert_date: str = "insert_date"
    latest_record_flag: str = "latest_record_flag"

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SCD2ColumnNames:
        """Create SCD2ColumnNames instance from dictionary, filtering valid fields."""
        field_names = {f.name for f in fields(cls)}
        filtered_data = {k: v for k, v in data.items() if k in field_names}
        return cls(**filtered_data)

    def field_list(self) -> list[str]:
        """Get list of all field attribute names."""
        return [f.name for f in fields(self)]

    def column_list(self) -> list[str]:
        """Get list of all actual output column names (respects user-defined renames)."""
        return [getattr(self, f.name) for f in fields(self)]


@dataclass
class SCD2Config:
    """Configuration for SCD2 processing."""

    business_keys: list[str]
    date_column: str = DEFAULT_DATE_COLUMN
    ignore_columns: list[str] | None = None
    non_copy_fields: list[str] | None = None
    open_end_date: datetime | None = OPEN_END_DATE
    scd_columns: SCD2ColumnNames = field(default_factory=SCD2ColumnNames)
    enable_latest_record_flag: bool = False
    source_type: SourceType = SourceType.FULL

    def __post_init__(self) -> None:
        """Initialize default values for optional fields."""
        if self.ignore_columns is None:
            self.ignore_columns = []
        if self.non_copy_fields is None:
            self.non_copy_fields = []

    @classmethod
    def create(
        cls,
        business_keys: list[str] | str,
        date_column: str = DEFAULT_DATE_COLUMN,
        ignore_columns: list[str] | None = None,
        non_copy_fields: list[str] | None = None,
        open_end_date: datetime | None = OPEN_END_DATE,
        scd_columns: SCD2ColumnNames | dict[str, str] | None = None,
        enable_latest_record_flag: bool = False,
        source_type: SourceType = SourceType.FULL,
    ) -> SCD2Config:
        """Create an SCD2Config instance with coercion and defaults applied.

        Args:
            business_keys: Column(s) that uniquely identify a dimension row.
                A single string is automatically wrapped in a list.
            date_column: Column containing the snapshot date.
            ignore_columns: Columns excluded from hash-based change detection.
            non_copy_fields: Source columns excluded from the output DataFrame.
            open_end_date: Value written to `valid_until` for currently active
                records. Defaults to `9999-12-31`.
            scd_columns: Override default SCD2 output column names. Accepts an
                `SCD2ColumnNames` instance or a plain dict with any subset of keys:
                `valid_from`, `valid_until`, `active_flag`, `delete_flag`, `row_hash`,
                `insert_date`.
            enable_latest_record_flag: When `True`, adds a `latest_record_flag` column
                that is `True` for the most recent record per business key.
            source_type: Whether the source is a ``"full"`` snapshot or
                ``"incremental"`` feed. Delete-flag detection is only performed
                for ``"full"`` sources; the ``delete_flag`` column is omitted
                entirely for ``"incremental"`` sources.
        """
        if isinstance(business_keys, str):
            business_keys = [business_keys]

        resolved_scd_columns: SCD2ColumnNames
        if isinstance(scd_columns, dict):
            resolved_scd_columns = SCD2ColumnNames.from_dict(scd_columns)
        elif scd_columns is None:
            resolved_scd_columns = SCD2ColumnNames()
        else:
            resolved_scd_columns = scd_columns

        return SCD2Config(
            business_keys=business_keys,
            date_column=date_column,
            ignore_columns=ignore_columns or [],
            non_copy_fields=non_copy_fields or [],
            open_end_date=open_end_date,
            scd_columns=resolved_scd_columns,
            enable_latest_record_flag=enable_latest_record_flag,
            source_type=source_type,
        )
