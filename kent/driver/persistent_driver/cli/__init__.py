"""Command-line interface for LocalDevDriverDebugger.

This module provides a Click-based CLI for inspecting and manipulating
LocalDevDriver run databases.

Usage:
    ldd-debug --db run.db info                    # Show run metadata and stats
    ldd-debug --db run.db requests list           # List requests
    ldd-debug --db run.db requests show <id>      # Show request details
    ldd-debug --db run.db responses list          # List responses
    ldd-debug --db run.db responses search        # Search response content
    ldd-debug --db run.db errors list             # List errors
    ldd-debug --db run.db results list            # List results
    ldd-debug --db run.db requeue request <id>    # Requeue a request
    ldd-debug --db run.db cancel request <id>     # Cancel a request
    ldd-debug --db run.db compression stats       # Show compression stats
    ldd-debug --db run.db diagnose <error-id>     # Diagnose an error
    ldd-debug --db run.db export jsonl <output>   # Export results to JSONL
    ldd-debug --db run.db export warc <output>    # Export responses to WARC
    ldd-debug --db run.db doctor health           # Run health checks
    ldd-debug --db run.db doctor structure        # Validate response structure

The --db option can be placed at any level:
    ldd-debug --db run.db doctor structure
    ldd-debug doctor --db run.db structure
    ldd-debug doctor structure --db run.db

All commands support:
    --format table|json|jsonl    Output format (default: table)
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import click

from kent.driver.persistent_driver.debugger import (
    LocalDevDriverDebugger,
)

# =========================================================================
# Output Formatting
# =========================================================================


def format_output(
    data: Any, format_type: str = "table", headers: list[str] | None = None
) -> None:
    """Format and print output based on format type.

    Args:
        data: Data to format (dict, list of dicts, or list of objects)
        format_type: Output format ('table', 'json', 'jsonl')
        headers: Column headers for table format
    """
    if format_type == "json":
        click.echo(json.dumps(data, indent=2))
    elif format_type == "jsonl":
        if isinstance(data, list):
            for item in data:
                click.echo(json.dumps(item))
        else:
            click.echo(json.dumps(data))
    elif format_type == "table":
        if isinstance(data, dict):
            # Single record - display as key-value pairs
            for key, value in data.items():
                click.echo(f"{key}: {value}")
        elif isinstance(data, list) and data:
            # Multiple records - display as table
            if headers is None:
                # Auto-detect headers from first item
                first = data[0]
                if hasattr(first, "__dict__"):
                    headers = list(vars(first).keys())
                elif isinstance(first, dict):
                    headers = list(first.keys())
                else:
                    headers = []

            if headers:
                # Print header
                click.echo("  ".join(str(h).ljust(15) for h in headers))
                click.echo("-" * (len(headers) * 17))

                # Print rows
                for item in data:
                    if hasattr(item, "__dict__"):
                        row = [str(getattr(item, h, ""))[:15] for h in headers]
                    elif isinstance(item, dict):
                        row = [str(item.get(h, ""))[:15] for h in headers]
                    else:
                        row = [str(item)[:15]]
                    click.echo("  ".join(v.ljust(15) for v in row))
            else:
                # Just print items
                for item in data:
                    click.echo(item)
        elif not data:
            click.echo("No results")
        else:
            click.echo(str(data))
    else:
        raise ValueError(f"Unknown format: {format_type}")


# =========================================================================
# Data Diff Formatting
# =========================================================================


def _format_data_diff(orig: dict[str, Any], new: dict[str, Any]) -> str:
    """Format the diff between two data dicts using jsondiff.

    Returns a human-readable diff showing changed fields.
    Aggregates list item changes to show field-level summary.
    Handles type changes (e.g., ConnTrialCourtDocket -> ConnTrialCaseUnavailable).
    """
    import jsondiff

    # Detect type change by checking if the sets of keys are fundamentally different
    # This catches cases like ConnTrialCourtDocket -> ConnTrialCaseUnavailable
    orig_keys = set(orig.keys())
    new_keys = set(new.keys())

    # If there's very little overlap in keys, it's likely a type change
    common_keys = orig_keys & new_keys
    all_keys = orig_keys | new_keys

    # If less than 30% of keys are shared, treat as type change
    if all_keys and len(common_keys) / len(all_keys) < 0.3:
        return (
            f"      Result type changed:\n"
            f"      - Removed fields: {sorted(orig_keys - new_keys)}\n"
            f"      + Added fields: {sorted(new_keys - orig_keys)}"
        )

    diff = jsondiff.diff(orig, new, syntax="symmetric")
    if not diff:
        return ""

    # Format the diff with aggregation
    return _format_jsondiff_aggregated(diff, indent=6)


def _format_jsondiff_aggregated(diff: Any, indent: int = 0) -> str:
    """Format jsondiff output with aggregation for list items.

    Groups similar changes across list items to produce concise output like:
    - date_filed: str -> datetime.date (all 50 entries)
    - description: None -> various values (35 entries)
    """
    lines: list[str] = []
    prefix = " " * indent

    if not isinstance(diff, dict):
        return f"{prefix}{_truncate_repr(diff)}"

    import jsondiff

    # Separate scalar changes from list changes
    scalar_changes: list[str] = []
    list_changes: dict[
        str, dict[int, dict[str, Any]]
    ] = {}  # list_name -> {idx -> changes}

    for key, value in diff.items():
        if key == jsondiff.symbols.insert:
            # Handle inserts - can be list of tuples or dict when type changes
            if isinstance(value, dict):
                # Type change scenario - show as nested dict
                nested = _format_jsondiff_aggregated(value, indent)
                if nested:
                    scalar_changes.append("+ inserted:")
                    scalar_changes.append(nested.lstrip())
            elif isinstance(value, list):
                try:
                    for pos, val in value:
                        scalar_changes.append(
                            f"+ [{pos}]: {_truncate_repr(val)}"
                        )
                except (ValueError, TypeError):
                    # If unpacking fails, just show the value as-is
                    scalar_changes.append(f"+ {_truncate_repr(value)}")
            else:
                scalar_changes.append(f"+ {_truncate_repr(value)}")
        elif key == jsondiff.symbols.delete:
            if isinstance(value, list):
                for pos in value:
                    scalar_changes.append(f"- [{pos}]")
            else:
                scalar_changes.append(f"- deleted: {_truncate_repr(value)}")
        elif isinstance(key, str) and isinstance(value, dict):
            # Check if this is a list with indexed changes
            if all(
                isinstance(k, int)
                for k in value
                if k not in (jsondiff.symbols.insert, jsondiff.symbols.delete)
            ):
                # This is a list field with changes
                list_changes[key] = value
            elif isinstance(value, list) and len(value) == 2:
                # Scalar field change
                scalar_changes.append(
                    f"{key}: {_truncate_repr(value[0])} \u2192 {_truncate_repr(value[1])}"
                )
            else:
                # Nested dict, recurse
                nested = _format_jsondiff_aggregated(value, indent)
                if nested:
                    scalar_changes.append(f"{key}:")
                    scalar_changes.append(nested.lstrip())
        elif isinstance(value, list) and len(value) == 2:
            # Scalar field with [old, new]
            scalar_changes.append(
                f"{key}: {_truncate_repr(value[0])} \u2192 {_truncate_repr(value[1])}"
            )
        else:
            scalar_changes.append(f"{key}: {_truncate_repr(value)}")

    # Output scalar changes
    for change in scalar_changes:
        lines.append(f"{prefix}{change}")

    # Aggregate list changes by field
    for list_name, item_changes in list_changes.items():
        # Collect all field changes across items
        field_stats: dict[
            str, dict[str, Any]
        ] = {}  # field -> {count, sample_old, sample_new, all_same}

        for idx, changes in item_changes.items():
            if isinstance(idx, int) and isinstance(changes, dict):
                for field, change_val in changes.items():
                    if isinstance(change_val, list) and len(change_val) == 2:
                        old_val, new_val = change_val
                        if field not in field_stats:
                            field_stats[field] = {
                                "count": 0,
                                "sample_old": old_val,
                                "sample_new": new_val,
                                "all_same": True,
                            }
                        field_stats[field]["count"] += 1
                        # Check if all values are the same pattern
                        if _type_name(old_val) != _type_name(
                            field_stats[field]["sample_old"]
                        ) or _type_name(new_val) != _type_name(
                            field_stats[field]["sample_new"]
                        ):
                            field_stats[field]["all_same"] = False

        # Output aggregated changes
        total_items = len(
            [idx for idx in item_changes if isinstance(idx, int)]
        )
        lines.append(f"{prefix}{list_name}: {total_items} items changed")

        for field, stats in sorted(field_stats.items()):
            count = stats["count"]
            sample_old = stats["sample_old"]
            sample_new = stats["sample_new"]

            if stats["all_same"]:
                # All changes are the same pattern (e.g., str -> date)
                old_type = _type_name(sample_old)
                new_type = _type_name(sample_new)
                if old_type != new_type:
                    lines.append(
                        f"{prefix}  .{field}: {old_type} \u2192 {new_type} ({count}x)"
                    )
                else:
                    lines.append(
                        f"{prefix}  .{field}: {_truncate_repr(sample_old)} \u2192 {_truncate_repr(sample_new)} ({count}x)"
                    )
            else:
                # Mixed changes
                lines.append(f"{prefix}  .{field}: various changes ({count}x)")

    return "\n".join(lines)


def _type_name(value: Any) -> str:
    """Get a short type name for a value."""
    if value is None:
        return "None"
    return type(value).__name__


def _truncate_repr(value: Any, max_len: int = 60) -> str:
    """Get a truncated repr of a value."""
    if value is None:
        return "None"
    s = repr(value)
    if len(s) > max_len:
        return s[: max_len - 3] + "..."
    return s


# =========================================================================
# CLI Groups
# =========================================================================


@click.group()
@click.version_option()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.pass_context
def cli(ctx: click.Context, db_path: str | None) -> None:
    """LocalDevDriver Debugger - Inspect and manipulate scraper run databases."""
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = db_path


# =========================================================================
# Shared Helpers
# =========================================================================


def _resolve_db_path(ctx: click.Context, db_path: str | None) -> str:
    """Resolve db_path from the current option or any parent group.

    Checks the subcommand's own --db first, then walks up the context
    chain checking ctx.obj["db_path"] (set by groups) and ctx.params.
    Raises UsageError if no --db was provided at any level.
    """
    if db_path:
        return db_path
    # Walk up to find --db from parent groups
    parent = ctx.parent
    while parent is not None:
        # Check ctx.obj (where groups store propagated values)
        obj = parent.ensure_object(dict)
        if obj.get("db_path"):
            return obj["db_path"]
        # Check params directly
        parent_db = parent.params.get("db_path")
        if parent_db:
            return parent_db
        parent = parent.parent
    raise click.UsageError(
        "Missing --db option. Provide a database path with --db."
    )


# =========================================================================
# Info Command
# =========================================================================


@cli.command()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def info(ctx: click.Context, db_path: str | None, format_type: str) -> None:
    """Show run metadata and statistics.

    \b
    Examples:
        ldd-debug info run.db
        ldd-debug info run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            metadata = await debugger.get_run_metadata()
            stats = await debugger.get_stats()

            if format_type == "table":
                click.echo("=== Run Metadata ===")
                if metadata:
                    for key, value in metadata.items():
                        click.echo(f"{key}: {value}")
                else:
                    click.echo("No metadata found")

                click.echo("\n=== Statistics ===")
                click.echo(f"Queue Total: {stats['queue']['total']}")
                click.echo(f"Queue Pending: {stats['queue']['pending']}")
                click.echo(f"Queue Completed: {stats['queue']['completed']}")
                click.echo(f"Queue Failed: {stats['queue']['failed']}")
                click.echo(f"Results Total: {stats['results']['total']}")
                click.echo(f"Errors Total: {stats['errors']['total']}")
                click.echo(
                    f"Errors Unresolved: {stats['errors']['unresolved']}"
                )
            else:
                output = {"metadata": metadata, "stats": stats}
                format_output(output, format_type)

    asyncio.run(run())


# =========================================================================
# Main Entry Point
# =========================================================================


def main() -> None:
    """Main CLI entry point."""
    cli()


# =========================================================================
# Register all subcommand modules
# (imports trigger @cli.group/@cli.command decorators)
# =========================================================================
from kent.driver.persistent_driver.cli import (
    compare as _compare_mod,
)
from kent.driver.persistent_driver.cli import (
    compression as _compression_mod,
)
from kent.driver.persistent_driver.cli import (
    doctor as _doctor_mod,
)
from kent.driver.persistent_driver.cli import (
    errors as _errors_mod,
)
from kent.driver.persistent_driver.cli import (
    export as _export_mod,
)
from kent.driver.persistent_driver.cli import (
    incidental as _incidental_mod,
)
from kent.driver.persistent_driver.cli import (
    requests as _requests_mod,
)
from kent.driver.persistent_driver.cli import (
    requeue as _requeue_mod,
)
from kent.driver.persistent_driver.cli import (
    responses as _responses_mod,
)
from kent.driver.persistent_driver.cli import (
    results as _results_mod,
)

if __name__ == "__main__":
    main()
