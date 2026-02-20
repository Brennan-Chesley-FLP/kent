"""CLI commands for inspecting incidental requests (browser-initiated network requests)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Incidental Requests Commands
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
def incidental(ctx: click.Context, db_path: str | None) -> None:
    """Inspect incidental requests (browser-initiated network requests)."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@incidental.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--parent-id", type=int, help="Filter by parent request ID")
@click.option(
    "--resource-type",
    help="Filter by resource type (e.g., script, stylesheet, image)",
)
@click.option(
    "--from-cache/--not-from-cache",
    default=None,
    help="Filter by cache status",
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
def incidental_list(
    ctx: click.Context,
    db_path: str | None,
    parent_id: int | None,
    resource_type: str | None,
    from_cache: bool | None,
    limit: int,
    offset: int,
    format_type: str,
) -> None:
    """List incidental requests with optional filtering.

    \b
    Examples:
        ldd-debug incidental list run.db
        ldd-debug incidental list run.db --parent-id 123
        ldd-debug incidental list run.db --resource-type script
        ldd-debug incidental list run.db --from-cache
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_incidental_requests(
                parent_request_id=parent_id,
                resource_type=resource_type,
                from_cache=from_cache,
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
                        "parent_id",
                        "type",
                        "url",
                        "status",
                        "cached",
                    ]
                    items = [
                        {
                            "id": r["id"],
                            "parent_id": r["parent_request_id"],
                            "type": r["resource_type"],
                            "url": r["url"][:40] if r["url"] else "",
                            "status": r["status_code"] or "failed",
                            "cached": "\u2713" if r["from_cache"] else "",
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No incidental requests found")
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


@incidental.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("incidental_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def incidental_show(
    ctx: click.Context,
    db_path: str | None,
    incidental_id: int,
    format_type: str,
) -> None:
    """Show detailed incidental request information.

    \b
    Examples:
        ldd-debug incidental show run.db 456
        ldd-debug incidental show run.db 456 --format json
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            inc = await debugger.get_incidental_request(incidental_id)

            if inc is None:
                click.echo(
                    f"Incidental request {incidental_id} not found", err=True
                )
                sys.exit(1)

            if format_type == "table":
                click.echo(f"ID: {inc['id']}")
                click.echo(f"Parent Request ID: {inc['parent_request_id']}")
                click.echo(f"Resource Type: {inc['resource_type']}")
                click.echo(f"Method: {inc['method']}")
                click.echo(f"URL: {inc['url']}")
                if inc["status_code"]:
                    click.echo(f"Status Code: {inc['status_code']}")
                if inc["content_size_original"]:
                    click.echo(
                        f"Original Size: {inc['content_size_original']} bytes"
                    )
                if inc["content_size_compressed"]:
                    click.echo(
                        f"Compressed Size: {inc['content_size_compressed']} bytes"
                    )
                click.echo(
                    f"From Cache: {'Yes' if inc['from_cache'] else 'No'}"
                )
                if inc["failure_reason"]:
                    click.echo(f"Failure Reason: {inc['failure_reason']}")
                click.echo(f"Created At: {inc['created_at']}")
            else:
                format_output(inc, format_type)

    asyncio.run(run())


@incidental.command("content")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("incidental_id", type=int)
@click.option("--output", "-o", help="Output file path (default: stdout)")
@click.pass_context
def incidental_content(
    ctx: click.Context,
    db_path: str | None,
    incidental_id: int,
    output: str | None,
) -> None:
    """Get decompressed incidental request content.

    \b
    Examples:
        ldd-debug incidental content run.db 456
        ldd-debug incidental content run.db 456 -o script.js
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            content = await debugger.get_incidental_request_content(
                incidental_id
            )

            if content is None:
                click.echo(
                    f"Incidental request {incidental_id} not found or has no content",
                    err=True,
                )
                sys.exit(1)

            if output:
                Path(output).write_bytes(content)
                click.echo(f"Content saved to {output}")
            else:
                # Try to decode as UTF-8, fall back to binary output
                try:
                    click.echo(content.decode("utf-8"))
                except UnicodeDecodeError:
                    click.echo(content, nl=False)

    asyncio.run(run())
