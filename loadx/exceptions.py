from __future__ import annotations


class SCD2Error(Exception):
    """Base exception class for SCD2 processing errors."""

    def __init__(self, message: str) -> None:
        super().__init__(message)
        self.message = message


class OldDataExceptionError(SCD2Error):
    """Exception raised when source data is older than target data."""

    def __init__(
        self, src_date: str | None = None, tgt_date: str | None = None
    ) -> None:
        if src_date and tgt_date:
            message = f"Source data ({src_date}) is older than target data ({tgt_date})"
        else:
            message = "Data is older than last target load date"
        super().__init__(message)
        self.src_date = src_date
        self.tgt_date = tgt_date


class EmptyDataExceptionError(SCD2Error):
    """Exception raised when source DataFrame is empty."""

    def __init__(self) -> None:
        super().__init__("Empty dataframe, exiting scd2 load")


class BusinessKeysEmptyError(ValueError, SCD2Error):
    """Exception raised when business keys are empty or not provided."""

    def __init__(self) -> None:
        super().__init__("business_keys cannot be empty")


class ConfigurationError(SCD2Error, ValueError):
    """Exception raised when SCD2 configuration is invalid."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Configuration error: {message}")


class DataValidationError(SCD2Error, ValueError):
    """Exception raised when data validation fails."""

    def __init__(self, message: str) -> None:
        super().__init__(f"Data validation error: {message}")
