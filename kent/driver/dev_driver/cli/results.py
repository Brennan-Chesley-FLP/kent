"""CLI commands for inspecting and exporting results."""

from __future__ import annotations

import asyncio
import json
import sys

import click

from kent.driver.dev_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Results Commands
# =========================================================================


@cli.group()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.pass_context
def results(ctx: click.Context, db_path: str | None) -> None:
    """Inspect and export results."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@results.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--type", "result_type", help="Filter by result type")
@click.option(
    "--valid/--invalid", default=None, help="Filter by validation status"
)
@click.option("--limit", default=100, help="Maximum number of results")
@click.option("--offset", default=0, help="Number of results to skip")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def results_list(
    ctx: click.Context,
    db_path: str | None,
    result_type: str | None,
    valid: bool | None,
    limit: int,
    offset: int,
    format_type: str,
) -> None:
    """List results with optional filtering.

    \b
    Examples:
        ldd-debug results list run.db
        ldd-debug results list run.db --type CourtOpinion --valid
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_results(
                result_type=result_type,
                is_valid=valid,
                limit=limit,
                offset=offset,
            )

            if format_type == "table":
                click.echo(
                    f"Total: {page.total}, Showing: {len(page.items)}, "
                    f"Offset: {offset}, Limit: {limit}"
                )
                if page.items:
                    headers = ["id", "type", "valid", "request_id"]
                    items = [
                        {
                            "id": r.id,
                            "type": r.result_type,
                            "valid": "\u2713" if r.is_valid else "\u2717",
                            "request_id": r.request_id,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No results found")
            else:
                output = {
                    "total": page.total,
                    "items": [
                        {
                            "id": r.id,
                            "request_id": r.request_id,
                            "result_type": r.result_type,
                            "is_valid": r.is_valid,
                            "data": r.data,
                            "validation_errors": r.validation_errors,
                        }
                        for r in page.items
                    ],
                    "limit": limit,
                    "offset": offset,
                    "has_more": page.has_more,
                }
                format_output(output, format_type)

    asyncio.run(run())


@results.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("result_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def results_show(
    ctx: click.Context, db_path: str | None, result_id: int, format_type: str
) -> None:
    """Show detailed result information.

    \b
    Examples:
        ldd-debug results show run.db 123
        ldd-debug results show run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_result(result_id)

            if result is None:
                click.echo(f"Result {result_id} not found", err=True)
                sys.exit(1)

            if format_type == "table":
                click.echo(f"ID: {result.id}")
                click.echo(f"Request ID: {result.request_id}")
                click.echo(f"Type: {result.result_type}")
                click.echo(f"Valid: {'Yes' if result.is_valid else 'No'}")
                click.echo(f"Data: {json.dumps(result.data, indent=2)}")
                if result.validation_errors:
                    click.echo(
                        f"Validation Errors: {json.dumps(result.validation_errors, indent=2)}"
                    )
                click.echo(f"Created At: {result.created_at}")
            else:
                output = {
                    "id": result.id,
                    "request_id": result.request_id,
                    "result_type": result.result_type,
                    "is_valid": result.is_valid,
                    "data": result.data,
                    "validation_errors": result.validation_errors,
                    "created_at": result.created_at,
                }
                format_output(output, format_type)

    asyncio.run(run())


@results.command("summary")
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
def results_summary(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show result counts by type and validity.

    \b
    Examples:
        ldd-debug results summary run.db
        ldd-debug results summary run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_result_summary()

            if format_type == "table":
                for result_type, counts in summary.items():
                    click.echo(f"\n{result_type}:")
                    for status, count in counts.items():
                        click.echo(f"  {status}: {count}")
            else:
                format_output(summary, format_type)

    asyncio.run(run())
