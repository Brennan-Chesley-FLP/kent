---
name: debug-scraper
description: Debug kent/pdd scrapers using the pdd CLI and database inspection tools. Use when the user reports scraper failures, XPath issues, missing results, or wants to investigate a scraper run database.
allowed-tools: Bash, Read, Grep, Glob, Write, Edit
---

# Debug Scraper

You are debugging a scraper built on the kent framework. You have access to
the `pdd` (persistent driver debugger) CLI for inspecting scraper run
databases, and the `kent` CLI for running and inspecting scrapers.

Always use `uv run pdd` and `uv run kent` to invoke these tools.

## Available CLI Tools

See [cli-reference.md](cli-reference.md) for the full CLI reference with all
commands, subcommands, and options.

## Debugging Workflow

Follow this general approach, adapting to the specific issue:

### 1. Understand the situation

- Ask what scraper and what symptom the user is seeing (if not already clear).
- Locate the run database (`.db` file). Ask the user if not obvious.
- Get an overview: `uv run pdd --db <path> info`

### 2. Inspect the run

- Check request status distribution: `uv run pdd --db <path> requests summary`
- Check error summary: `uv run pdd --db <path> errors summary`
- Check result summary: `uv run pdd --db <path> results summary`

### 3. Diagnose errors

- List errors: `uv run pdd --db <path> errors list`
- Show specific error details: `uv run pdd --db <path> errors show <id>`
- Re-run XPath observation on an error: `uv run pdd --db <path> errors diagnose <error-id>`

### 4. Inspect XPath behavior across pages

- Bulk XPath stats for a continuation: `uv run pdd --db <path> bulk-xpath <continuation> --sample 20`
- This shows which selectors fail, how often, and match count distributions.

### 5. Compare code changes

- Compare current code against stored results: `uv run pdd --db <path> step re-evaluate <step_name>`
- Use `--verbose` for per-request diffs.
- Use `--sample N` to spot-check a subset.

### 6. Inspect specific requests/responses

- Show request details: `uv run pdd --db <path> requests show <id>`
- Get response content: `uv run pdd --db <path> responses content <request-id>`
- Search responses: `uv run pdd --db <path> responses search --text "pattern"`

### 7. Read the scraper source

- Use `uv run kent inspect <scraper-module>` to see the scraper's metadata and
  entry points.
- Read the continuation methods directly in the source code to understand what
  XPath selectors are being used and what data is being extracted.

## Important Notes

- The `--db` flag can go at any level: `pdd --db run.db errors list` or
  `pdd errors --db run.db list` or `pdd errors list --db run.db`.
- All pdd commands work on SQLite databases produced by kent's persistent
  driver (LocalDevDriver).
- When inspecting response content, look for HTML structure changes that would
  break XPath selectors.
- If `diagnose` shows selectors failing, check the response content to see if
  the HTML structure has changed.

## After Debugging: Incident Report

After completing a debugging session, notify the user that you are writing a
brief incident report, then write it to:

    .claude/debug-incidents/<YYYY-MM-DD>-<short-slug>.md

Use this template:

```markdown
# <Short title>

- **Date**: <YYYY-MM-DD>
- **Scraper**: <scraper module or name>
- **Symptom**: <what the user reported>
- **Root cause**: <what was actually wrong>
- **pdd tools used**: <which pdd commands helped, briefly>
- **Gaps**: <any steps where pdd didn't have the right tool and you had to
  fall back on manual inspection, raw file reading, shell commands, etc.
  Write "None" if pdd covered everything.>
- **Suggested improvement**: <one-sentence idea for a pdd feature that would
  have helped, or "None">
```

Keep each report to roughly 10-15 lines. These reports are read in aggregate
to iteratively improve pdd's debugging capabilities.
