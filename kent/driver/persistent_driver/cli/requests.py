"""CLI commands for inspecting and manipulating requests."""

from __future__ import annotations

import asyncio

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Requests Commands
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
def requests(ctx: click.Context, db_path: str | None) -> None:
    """Inspect and manipulate requests."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@requests.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option(
    "--status", help="Filter by status (pending, completed, failed, held)"
)
@click.option("--continuation", help="Filter by continuation (step name)")
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
def requests_list(
    ctx: click.Context,
    db_path: str | None,
    status: str | None,
    continuation: str | None,
    limit: int,
    offset: int,
    format_type: str,
) -> None:
    """List requests with optional filtering.

    \b
    Examples:
        ldd-debug requests list run.db
        ldd-debug requests list run.db --status failed
        ldd-debug requests list run.db --continuation step1 --limit 50
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(
                status=status,  # type: ignore
                continuation=continuation,
                limit=limit,
                offset=offset,
            )

            if format_type == "table":
                click.echo(
                    f"Total: {page.total}, Showing: {len(page.items)}, "
                    f"Offset: {offset}, Limit: {limit}"
                )
                if page.items:
                    headers = [
                        "id",
                        "status",
                        "url",
                        "continuation",
                        "retry_count",
                    ]
                    items = [
                        {
                            "id": r.id,
                            "status": r.status,
                            "url": r.url[:50] if r.url else "",
                            "continuation": r.continuation,
                            "retry_count": r.retry_count,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No requests found")
            else:
                output = {
                    "total": page.total,
                    "items": [
                        {
                            "id": r.id,
                            "status": r.status,
                            "url": r.url,
                            "continuation": r.continuation,
                            "retry_count": r.retry_count,
                            "method": r.method,
                            "priority": r.priority,
                        }
                        for r in page.items
                    ],
                    "limit": limit,
                    "offset": offset,
                    "has_more": page.has_more,
                }
                format_output(output, format_type)

    asyncio.run(run())


@requests.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("request_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def requests_show(
    ctx: click.Context, db_path: str | None, request_id: int, format_type: str
) -> None:
    """Show detailed request information.

    \b
    Examples:
        ldd-debug requests show run.db 123
        ldd-debug requests show run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            request = await debugger.get_request(request_id)

            if request is None:
                click.echo(f"Request {request_id} not found", err=True)
                import sys

                sys.exit(1)

            if format_type == "table":
                click.echo(f"ID: {request.id}")
                click.echo(f"Status: {request.status}")
                click.echo(f"URL: {request.url}")
                click.echo(f"Method: {request.method}")
                click.echo(f"Continuation: {request.continuation}")
                click.echo(f"Priority: {request.priority}")
                click.echo(f"Retry Count: {request.retry_count}")
                click.echo(f"Created At: {request.created_at}")
                if request.completed_at:
                    click.echo(f"Completed At: {request.completed_at}")
            else:
                output = {
                    "id": request.id,
                    "status": request.status,
                    "url": request.url,
                    "method": request.method,
                    "continuation": request.continuation,
                    "priority": request.priority,
                    "retry_count": request.retry_count,
                    "created_at": request.created_at,
                    "completed_at": request.completed_at,
                }
                format_output(output, format_type)

    asyncio.run(run())


@requests.command("summary")
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
def requests_summary(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show request counts by status and continuation.

    \b
    Examples:
        ldd-debug requests summary run.db
        ldd-debug requests summary run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_request_summary()

            if format_type == "table":
                for continuation, status_counts in summary.items():
                    click.echo(f"\n=== {continuation} ===")
                    for status, count in status_counts.items():
                        click.echo(f"  {status}: {count}")
            else:
                format_output(summary, format_type)

    asyncio.run(run())
