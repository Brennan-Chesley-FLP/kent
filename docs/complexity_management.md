# Complexity Management

Strategies and tools for managing code complexity in kent.

## Current linting setup

Ruff is the primary linter with these rule categories enabled: B (bugbear), C4 (comprehensions), E/W (pycodestyle), F (pyflakes), I (isort), SIM (simplify), TID (tidy-imports), UP (pyupgrade). Ruff-format handles code formatting. MyPy runs in pre-commit for type checking, and pyright is used for deeper type analysis via `scripts/check-types.sh`.

## Complexity analysis in ruff

### C90 (McCabe cyclomatic complexity)

Flags functions with too many branches/paths. Configured via:

```toml
[tool.ruff.lint]
select = ["B", "C4", "C90", "E", "F", "I", "SIM", "TID", "UP", "W"]

[tool.ruff.lint.mccabe]
max-complexity = 15
```

Start lenient (15) and tighten over time. The default is 10.

### PLR0912 / PLR0915 (pylint branch/statement limits)

Complementary to C90. Cap the number of branches and statements per function:

```toml
[tool.ruff.lint]
select = [..., "PLR0912", "PLR0915"]

[tool.ruff.lint.pylint]
max-branches = 15
max-statements = 60
```

## Additional ruff rule sets to consider

| Category | Code | What it catches |
|---|---|---|
| McCabe complexity | C90 | Functions with too many branches |
| Pylint | PL (PLC/PLE/PLR/PLW) | Broad set including complexity rules |
| Return style | RET | Unnecessary else after return, superfluous assignments |
| Raise style | RSE | Unnecessary parens on exceptions |
| flake8-pie | PIE | Misc simplifications (unnecessary pass, dict.setdefault) |
| Perflint | PERF | Performance anti-patterns (unnecessary list() in iteration) |
| Refurb | FURB | Modernization suggestions beyond pyupgrade |
| flake8-logging | LOG | Logging anti-patterns |
| Ruff-specific | RUF | Mutable defaults, unused noqa, etc. |

Low-noise, high-value additions: **C90, RUF, PERF, RET**.

## Tools beyond ruff

### Radon — complexity reporting

Purpose-built complexity analyzer. Produces ranked reports rather than pass/fail:

- **Cyclomatic complexity** per function (grades A-F)
- **Maintainability index** per module (0-100)
- **Halstead metrics** (operand/operator complexity)

```bash
uv run radon cc kent/ -a -s -n C   # all functions graded C or worse
uv run radon mi kent/ -s            # maintainability index per module
```

Better than C90 for exploration because it ranks the whole codebase.

### Xenon — CI complexity enforcement

Wraps radon to enforce thresholds in CI:

```bash
uv run xenon kent/ --max-absolute B --max-modules A --max-average A
```

### Vulture — dead code detection

Finds unused functions, variables, and imports. Useful during consolidation work.

```bash
uv run vulture kent/
```

### Cognitive complexity

Measures how hard code is to *understand* rather than branch count. A deeply nested loop scores higher than many flat if-statements, better matching human difficulty. Available via `flake8-cognitive-complexity`.

## Rollout approach

1. Enable C90 at max-complexity=15 (lenient) to see what lights up
2. Fix or suppress the worst offenders
3. Tighten to 12, then 10 over time
4. Add RUF, PERF, RET as low-noise next steps
5. Run radon periodically for a full complexity snapshot
6. Add PLR0912/PLR0915 once the C90 baseline is clean
