"""CLI commands for inspecting incidental requests (browser-initiated network requests)."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli.templating import render_output
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
    type=click.Choice(["default", "summary", "table", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
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
    template_name: str | None,
) -> None:
    """List incidental requests with optional filtering.

    \b
    Examples:
        pdd incidental list --db run.db
        pdd incidental list --db run.db --parent-id 123
        pdd incidental list --db run.db --resource-type script
        pdd incidental list --db run.db --from-cache
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

            output = {
                "total": page.total,
                "items": [
                    {
                        "id": r.id,
                        "parent_id": r.parent_request_id,
                        "type": r.resource_type,
                        "url": r.url[:40] if r.url else "",
                        "status": r.status_code or "failed",
                        "cached": "\u2713" if r.from_cache else "",
                    }
                    for r in page.items
                ],
                "limit": limit,
                "offset": offset,
                "has_more": page.has_more,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="incidental/list",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "table", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def incidental_show(
    ctx: click.Context,
    db_path: str | None,
    incidental_id: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show detailed incidental request information.

    \b
    Examples:
        pdd incidental show --db run.db 456
        pdd incidental show --db run.db 456 --format json
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

            render_output(
                inc.to_dict(),
                format_type=format_type,
                template_path="incidental/show",
                template_name=template_name or "default",
            )

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
        pdd incidental content --db run.db 456
        pdd incidental content --db run.db 456 -o script.js
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
