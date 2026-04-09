---
name: python-development-standards
description: Apply Python development standards including uv project management, OOP principles, DRY, and clean code best practices. Use when creating new Python modules, refactoring existing code, or reviewing Python code quality.
---

# Python Development Standards

Apply these standards when writing, refactoring, or reviewing Python code in this project.

## Project Management: uv

All Python project operations use **uv** (not pip, poetry, or pipenv).

```bash
# Environment setup
uv sync --all-extras          # Install all dependencies including extras
uv sync                       # Install base dependencies only

# Running code
uv run python script.py       # Run a script in the project environment
uv run pytest tests/ -v       # Run tests
uv run mypy loadx/            # Type checking
uv run pre-commit run --all-files  # Linting/formatting

# Dependency management
uv add <package>              # Add a runtime dependency
uv add --dev <package>        # Add a dev dependency
uv remove <package>           # Remove a dependency
uv lock                       # Update lockfile
uv build                      # Build the package

# Never use:
# pip install, poetry add, pipenv install — use uv equivalents instead
```

All dependencies are declared in `pyproject.toml`. The lockfile is `uv.lock`.

---

## Object-Oriented Programming Principles

### Classes: Single Responsibility
Each class does one thing. If a class name requires "and" to describe it, split it.

```python
# Bad — handles both config and processing
class DataProcessor:
    def validate_config(self): ...
    def process(self): ...
    def write_output(self): ...

# Good — separated concerns
class DataConfig:
    """Holds validated configuration for the data pipeline."""
    ...

class DataProcessor:
    """Transforms source data using a given config."""
    def __init__(self, config: DataConfig): ...
```

### Encapsulation
Keep internals private. Expose only what callers need.

```python
class SCD2Loader:
    def slowly_changing_dimension(self, ...):  # public API
        config = self._build_config(...)        # private
        return self._process(config)            # private

    def _build_config(self, ...): ...
    def _process(self, config): ...
```

### Composition over Inheritance
Prefer injecting collaborators over deep inheritance chains.

```python
# Prefer
class Loader:
    def __init__(self, validator: Validator, hasher: Hasher): ...

# Avoid
class Loader(BaseLoader, ValidatorMixin, HasherMixin): ...
```

### Dataclasses for Value Objects
Use `@dataclass` (or `@dataclass(frozen=True)`) for config and data containers. Avoid plain dicts for structured data.

```python
from dataclasses import dataclass, field

@dataclass(frozen=True)
class SCD2Config:
    business_keys: list[str]
    ignore_columns: list[str] = field(default_factory=list)
    open_end_date: str = "9999-12-31"
```

---

## DRY (Don't Repeat Yourself)

- Extract any logic used more than once into a named function or method.
- Constants defined once at module level, not repeated as string/number literals.
- Shared validation logic lives in one place; callers import it.

```python
# Bad — repeated filter logic
if df.filter(col("active_flag") == 1).count() == 0: ...
# ... elsewhere ...
if df.filter(col("active_flag") == 1).count() == 0: ...

# Good — extracted once
def has_active_records(df: DataFrame) -> bool:
    return df.filter(col("active_flag") == 1).count() > 0
```

---

## Clean Code Principles

### Naming
- Names reveal intent: `calculate_row_hash`, not `do_hash` or `h`.
- Boolean variables and functions: `is_`, `has_`, `should_` prefix.
- Avoid abbreviations unless universally understood (`df`, `cfg`, `idx`).

### Functions
- Do one thing. If it needs a comment to explain *what* it does (not *why*), rename or split it.
- Limit parameters. More than 3–4 suggests a config object is needed.
- No side effects in pure transform functions — return new data, don't mutate inputs.

```python
# Bad — mutates and has side effects
def add_hash(df):
    df["hash"] = compute_hash(df)  # mutates
    log.info("hash added")         # side effect
    return df

# Good — pure function
def apply_hash_column(df: DataFrame, columns: list[str]) -> DataFrame:
    return df.withColumn("row_hash", sha2(concat_ws("|", *columns), 256))
```

### Type Annotations
All public functions and methods must have full type annotations. Use `from __future__ import annotations` for forward references.

```python
from __future__ import annotations
from pyspark.sql import DataFrame

def filter_for_changes(df: DataFrame, hash_col: str) -> DataFrame: ...
```

### Error Handling
- Raise specific, meaningful exceptions from a custom hierarchy (see `loadx/exceptions.py`).
- Never silence exceptions with bare `except: pass`.
- Validate at system boundaries (user inputs, external data). Trust internal invariants.

```python
# Bad
try:
    result = process(df)
except Exception:
    pass

# Good
if not business_keys:
    raise ConfigurationError("business_keys must not be empty")
```

### Logging — Never Use print
Always use the standard `logging` module. `print` is not acceptable in library or application code.

Set up a logger once per module using the module's `__name__`:

```python
import logging

logger = logging.getLogger(__name__)
```

Use the appropriate level for each message:

```python
# Bad
print("Starting load...")
print(f"Processed {count} rows")
print("ERROR: missing keys")

# Good
logger.debug("Starting load with config: %s", config)
logger.info("Processed %d rows", count)
logger.warning("No active records found in target; treating as initial load")
logger.error("Missing required business keys: %s", missing_keys)
logger.exception("Unexpected failure during transform")  # includes traceback
```

Level guidelines:
- `DEBUG` — internal state, loop counters, intermediate values (verbose; off by default)
- `INFO` — high-level pipeline milestones (start, finish, record counts)
- `WARNING` — recoverable unexpected conditions
- `ERROR` — failures that prevent an operation from completing
- `EXCEPTION` — like ERROR but automatically appends the current traceback

Logger configuration (format, handlers, levels) is centralized in `loadx/utils/logging_config.py`. Modules must never call `logging.basicConfig()` or add handlers themselves.

### Comments
- Comment *why*, not *what*. The code shows what; comments explain non-obvious decisions.
- No commented-out code. Delete it; git history preserves it.
- No docstrings on private helpers unless the logic is genuinely subtle.

```python
# Bad
# Add hash column
df = df.withColumn("row_hash", sha2(...))

# Good
# Include delete_flag in hash so deletions trigger SCD2 rows even when data is unchanged
df = df.withColumn("row_hash_changed", sha2(concat_ws("|", *cols_with_flag), 256))
```

### Module Organization
Follow the project's existing layout:
- `loader.py` — public API only
- `transforms.py` — pure functions, no I/O
- `config.py` — dataclasses/config types
- `exceptions.py` — custom exception hierarchy

Keep imports ordered: stdlib → third-party → internal. Use `isort` (enforced by pre-commit).

---

## Checklist Before Submitting Code

- [ ] `uv run pytest tests/ -v` passes
- [ ] `uv run mypy loadx/` passes with no errors
- [ ] `uv run pre-commit run --all-files` passes
- [ ] No repeated logic — extract duplicates into functions
- [ ] All public APIs have type annotations
- [ ] New classes have a single, clear responsibility
- [ ] No bare `except`, no silenced errors
- [ ] No commented-out code left behind
