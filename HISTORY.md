Changelog
=========

Unreleased
----------

- Preserve field boundaries and null positions in change hashes. Hash values
  change; see the documentation's hash compatibility guidance.
- Reject output names that overwrite internal processing columns and preserve
  source attributes when SCD2 output columns are renamed.
- Support business keys that share names with validation aggregation aliases.
- Clear the previous latest flag when a deleted key reappears, preserving its
  historical attributes and dates. This mode requires the full target history
  and its SCD2 output columns.
- Run CI checks for every pull request and main-branch push.
- Align development and published dependencies on PySpark 3.5.x, and use the
  same Black version locally and in pre-commit. Remove unused overlapping tools.
