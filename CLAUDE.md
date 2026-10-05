# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dependencies
make install          # uv sync --all-extras

# Run tests with coverage
make test             # pytest with XML coverage output

# Run a single test file
uv run pytest tests/test_scd2_loader.py -v

# Run a single test
uv run pytest tests/test_scd2_loader.py::TestSCD2Load::test_initial_load -v

# All quality checks (uv lock, pre-commit, mypy, deptry)
make check

# Build package
make build

# Serve documentation locally
make docs-serve
```

## Architecture

This is a **PySpark library** for Slowly Changing Dimension Type 2 (SCD2) transformations. It tracks historical changes to dimensional data using valid_from/valid_until date ranges, hash-based change detection, and active/delete flags.

### Module Layout

```
loadx/
├── __init__.py              # exports SCD2Loader, SCD2ColumnNames, SourceType
├── exceptions.py            # Custom exception hierarchy
├── scd2/                    # SCD2 (history) strategy
│   ├── __init__.py          # exports SCD2Loader
│   ├── loader.py            # Public API: SCD2Loader + pipeline orchestration
│   ├── config.py            # SCD2Config, SCD2ColumnNames dataclasses
│   └── transforms.py        # All pure functions: validation, hashing, data transforms
├── scd1/                    # Future: SCD1 (merge/upsert) strategy
│   └── __init__.py          # stub with docstring only
├── overwrite/               # Future: overwrite strategy
│   └── __init__.py          # stub with docstring only
└── utils/
    ├── spark_factory.py     # SparkSession factory
    └── logging_config.py    # Centralized logging
```

### Data Flow

```
SCD2Loader.slowly_changing_dimension()
  → SCD2Config.create()               # Build config from parameters
  → SCD2Loader._process()
      → transforms.validate_config()          # Keys, source_type, output column name collisions
      → transforms.validate_inputs()          # Non-empty DataFrame, required columns
      → transforms.validate_source_columns()  # Reserved names, ignore/non_copy columns exist
      → transforms.validate_source_rows()     # Null keys/dates, conflicting rows per key + date
      → transforms.prepare_source_data()      # Add placeholder SCD2 columns
      → transforms.validate_target()          # Target columns, one active row per key (if target)
      → transforms.handle_incremental_load()  # Merge active target rows (if target)
      → transforms.process_deletions()        # Handle records deleted between snapshots
      → transforms.apply_hash_columns()       # SHA-256 hashes for change detection
      → transforms.filter_for_changes()       # Window-based lag comparison
      → transforms.add_support_columns()      # valid_from, valid_until, active_flag
      → transforms.finalize_output()          # Select final columns, add upsert_flag
```

### Key Design Decisions

- **Change detection**: Two hash columns — `row_hash` (content only) and `row_hash_changed` (includes delete flag). A record is considered changed when `row_hash_changed` differs from the previous snapshot via a window `lag()`.
- **Deletion handling**: Deletions are detected by comparing source business keys between consecutive snapshots. Deleted records get a `delete_flag=True` and a closed `valid_until` date.
- **Incremental loads**: When a `df_tgt` is passed, only its active records (`valid_until == open_end_date`) are merged with source data; closed history rows are excluded because they would tie on the target date in the date-ordered windows. Only changed records (by hash) flow through the pipeline.
- **SCD2 column names are configurable** via `SCD2ColumnNames` dataclass — all output column names can be overridden.
- **`ignore_columns`**: List of column names excluded from hash calculations (e.g., audit timestamps that shouldn't trigger SCD2 changes).
- **`OPEN_END_DATE`** = `9999-12-31` marks currently active records.

### Testing

Tests use `chispa` for PySpark DataFrame assertions. Session-scoped `spark` fixture in `conftest.py` is reused across all tests (local[1], test-optimized). `data_day1/day2/day3` fixtures simulate multi-day employee snapshots for integration tests.
