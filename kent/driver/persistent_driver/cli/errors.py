"""CLI commands for inspecting and manipulating errors."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Errors Commands
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
def errors(ctx: click.Context, db_path: str | None) -> None:
    """Inspect and manipulate errors."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@errors.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--type", "error_type", help="Filter by error type")
@click.option(
    "--resolved/--unresolved",
    default=None,
    help="Filter by resolution status",
)
@click.option("--step", help="Filter by step name")
@click.option("--limit", default=100, help="Maximum number of results")
@click.option("--offset", default=0, help="Number of results to skip")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def errors_list(
    ctx: click.Context,
    db_path: str | None,
    error_type: str | None,
    resolved: bool | None,
    step: str | None,
    limit: int,
    offset: int,
    format_type: str,
) -> None:
    """List errors with optional filtering.

    \b
    Examples:
        pdd errors list --db run.db
        pdd errors list --db run.db --type xpath --unresolved
        pdd errors list --db run.db --step step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_errors(
                error_type=error_type,
                is_resolved=resolved,
                continuation=step,
                limit=limit,
                offset=offset,
            )

            if format_type == "summary":
                click.echo(
                    f"Total: {page.total}, Showing: {len(page.items)}, "
                    f"Offset: {offset}, Limit: {limit}"
                )
                if page.items:
                    headers = ["id", "type", "message", "resolved"]
                    items = [
                        {
                            "id": e["id"],
                            "type": e["error_type"],
                            "message": e["message"][:50]
                            if e.get("message")
                            else "",
                            "resolved": "\u2713"
                            if e["is_resolved"]
                            else "\u2717",
                        }
                        for e in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No errors found")
            else:
                output = {
                    "total": page.total,
                    "items": page.items,
                    "limit": limit,
                    "offset": offset,
                    "has_more": page.has_more,
                }
                format_output(output, format_type)

    asyncio.run(run())


@errors.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("error_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def errors_show(
    ctx: click.Context, db_path: str | None, error_id: int, format_type: str
) -> None:
    """Show detailed error information.

    \b
    Examples:
        pdd errors show --db run.db 123
        pdd errors show --db run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            error = await debugger.get_error(error_id)

            if error is None:
                click.echo(f"Error {error_id} not found", err=True)
                sys.exit(1)

            if format_type == "summary":
                click.echo(f"ID: {error['id']}")
                click.echo(f"Type: {error['error_type']}")
                click.echo(f"Message: {error['message']}")
                click.echo(f"Request ID: {error['request_id']}")
                click.echo(
                    f"Resolved: {'Yes' if error['is_resolved'] else 'No'}"
                )
                if error.get("selector"):
                    click.echo(f"Selector: {error['selector']}")
                if error.get("resolution_notes"):
                    click.echo(
                        f"Resolution Notes: {error['resolution_notes']}"
                    )
                click.echo(f"Created At: {error['created_at']}")
            else:
                format_output(error, format_type)

    asyncio.run(run())


@errors.command("summary")
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def errors_summary(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show error counts by type and resolution status.

    \b
    Examples:
        pdd errors summary --db run.db
        pdd errors summary --db run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_error_summary()

            if format_type == "summary":
                click.echo("=== Totals ===")
                for key, value in summary["totals"].items():
                    click.echo(f"  {key}: {value}")

                click.echo("\n=== By Type ===")
                for error_type, counts in summary["by_type"].items():
                    click.echo(f"\n{error_type}:")
                    for status, count in counts.items():
                        click.echo(f"  {status}: {count}")

                if summary["by_continuation"]:
                    click.echo("\n=== By Continuation ===")
                    for continuation, count in summary[
                        "by_continuation"
                    ].items():
                        click.echo(f"  {continuation}: {count}")
            else:
                format_output(summary, format_type)

    asyncio.run(run())


@errors.command("resolve")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("error_id", type=int)
@click.option("--notes", help="Resolution notes")
@click.pass_context
def errors_resolve(
    ctx: click.Context, db_path: str | None, error_id: int, notes: str | None
) -> None:
    """Mark an error as resolved.

    \b
    Examples:
        pdd errors resolve --db run.db 123
        pdd errors resolve --db run.db 123 --notes "Fixed XPath selector"
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            resolved = await debugger.resolve_error(error_id, notes)

            if resolved:
                click.echo(f"Error {error_id} marked as resolved")
            else:
                click.echo(
                    f"Error {error_id} not found or already resolved", err=True
                )
                sys.exit(1)

    asyncio.run(run())


@errors.command("requeue")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("error_id", type=int)
@click.option("--notes", help="Resolution notes")
@click.pass_context
def errors_requeue(
    ctx: click.Context, db_path: str | None, error_id: int, notes: str | None
) -> None:
    """Requeue the request that caused an error.

    \b
    Examples:
        pdd errors requeue --db run.db 123
        pdd errors requeue --db run.db 123 --notes "Fixed server issue"
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                new_id = await debugger.requeue_error(error_id, notes)
                click.echo(f"Error {error_id} requeued as request {new_id}")
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())


@errors.command("requeue-all")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--type", "error_type", help="Filter by error type")
@click.option("--step", help="Filter by step name")
@click.pass_context
def errors_requeue_all(
    ctx: click.Context,
    db_path: str | None,
    error_type: str | None,
    step: str | None,
) -> None:
    """Batch requeue errors matching filter criteria.

    \b
    Examples:
        pdd errors requeue-all --db run.db --type xpath
        pdd errors requeue-all --db run.db --step step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.batch_requeue_errors(
                error_type=error_type, continuation=step
            )
            click.echo(f"Requeued {count} errors")

    asyncio.run(run())


# =========================================================================
# Diagnose Command (moved from compare.py)
# =========================================================================


@errors.command("diagnose")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("error_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def errors_diagnose(
    ctx: click.Context, db_path: str | None, error_id: int, format_type: str
) -> None:
    """Diagnose an error by re-running XPath observation.

    \b
    Examples:
        pdd errors diagnose --db run.db 123
        pdd errors diagnose --db run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            try:
                result = await debugger.diagnose(error_id)

                if format_type == "summary":
                    click.echo("=== Error ===")
                    click.echo(f"ID: {result['error']['id']}")
                    click.echo(f"Type: {result['error']['error_type']}")
                    click.echo(f"Message: {result['error']['message']}")

                    click.echo("\n=== Response ===")
                    click.echo(f"ID: {result['response']['id']}")
                    click.echo(f"Status: {result['response']['status_code']}")
                    click.echo(f"URL: {result['response']['url']}")
                    click.echo(f"Size: {result['response']['size']} bytes")

                    click.echo("\n=== Scraper ===")
                    if result["scraper_info"]["class"]:
                        click.echo(f"Class: {result['scraper_info']['class']}")
                        click.echo(
                            f"Module: {result['scraper_info']['module']}"
                        )

                    click.echo("\n=== Observations ===")
                    for key, value in result["observations"].items():
                        click.echo(f"{key}: {value}")
                else:
                    format_output(result, format_type)
            except (ValueError, ImportError) as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())
