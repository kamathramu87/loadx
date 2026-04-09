# PR Rules

Follow these rules every time you create a pull request for this project.

## Branch naming

- Features: `add-<short-description>` (e.g. `add-sql-generator`)
- Bug fixes: `fix-<short-description>`
- Docs: `docs-<short-description>`
- Refactors: `refactor-<short-description>`

## Pre-PR checklist

Before opening a PR, confirm all of the following:

- [ ] `make check` passes (ruff, black, mypy, deptry)
- [ ] `make test` passes with no failures
- [ ] New public methods have docstrings
- [ ] `__init__.py` exports updated if new public symbols were added

## PR title

Format: `<verb> <what> (<scope>)` — keep it under 72 characters.

Good verbs: Add, Fix, Refactor, Remove, Update

Examples:
- `Add SQL generation with multi-dialect support`
- `Fix incremental load when target table is empty`

## PR body

Use this template:

```
## Summary
- <bullet: what changed and why>

## Test plan
- [ ] <what was tested>
- [ ] All existing tests still pass (`make test`)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
```

## Scope rules

- One logical change per PR — don't bundle unrelated fixes
- Keep PRs small enough to review in one sitting
- If a migration or breaking change is included, call it out explicitly in the summary

## Now create the PR

Before branching, always sync with the latest main:

```bash
git checkout main
git pull origin main
git checkout -b <branch-name>
```

Then run `make check` and `make test`, commit, push, and open the PR using `gh pr create` following the rules above.
