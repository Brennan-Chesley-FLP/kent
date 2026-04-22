"""CLI commands for inspecting responses."""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli._options import (
    db_option,
    format_options,
    pagination_options,
    search_options,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Responses Commands
# =========================================================================


@cli.group()
@db_option
@click.pass_context
def responses(ctx: click.Context, db_path: str | None) -> None:
    """Inspect responses."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@responses.command("list")
@click.option("--continuation", help="Filter by continuation (step name)")
@db_option
@format_options
@pagination_options
@click.pass_context
def responses_list(
    ctx: click.Context,
    db_path: str | None,
    continuation: str | None,
    limit: int,
    offset: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """List responses with optional filtering.

    \b
    Examples:
        ldd-debug responses list run.db
        ldd-debug responses list run.db --continuation step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_responses(
                continuation=continuation, limit=limit, offset=offset
            )

            output = {
                "total": page.total,
                "items": [
                    {
                        "id": r.id,
                        "status_code": r.status_code,
                        "url": r.url,
                        "continuation": r.continuation,
                        "content_size_original": r.content_size_original,
                        "content_size_compressed": r.content_size_compressed,
                        "compression_ratio": r.compression_ratio,
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
                template_path="responses/list",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@responses.command("show")
@click.argument("request_id", type=int)
@db_option
@format_options
@click.pass_context
def responses_show(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show detailed response information.

    \b
    Examples:
        ldd-debug responses show run.db 123
        ldd-debug responses show run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            response = await debugger.get_response(request_id)

            if response is None:
                click.echo(
                    f"Response for request {request_id} not found", err=True
                )
                sys.exit(1)

            output = {
                "id": response.id,
                "status_code": response.status_code,
                "url": response.url,
                "continuation": response.continuation,
                "content_size_original": response.content_size_original,
                "content_size_compressed": response.content_size_compressed,
                "compression_ratio": response.compression_ratio,
                "created_at": response.created_at,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="responses/show",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@responses.command("content")
@click.argument("request_id", type=int)
@click.option("--output", "-o", help="Output file path (default: stdout)")
@db_option
@click.pass_context
def responses_content(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    output: str | None,
) -> None:
    """Get decompressed response content.

    \b
    Examples:
        ldd-debug responses content run.db 123
        ldd-debug responses content run.db 123 -o response.html
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            content = await debugger.get_response_content(request_id)

            if content is None:
                click.echo(
                    f"Response for request {request_id} not found", err=True
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


@responses.command("search")
@click.option("--continuation", help="Filter by continuation (step name)")
@db_option
@format_options
@search_options
@click.pass_context
def responses_search(
    ctx: click.Context,
    db_path: str | None,
    text_pattern: str | None,
    regex_pattern: str | None,
    xpath_expr: str | None,
    continuation: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """Search response content for matching patterns.

    Searches through all response content (decompressed) for matches.
    Exactly one of --text, --regex, or --xpath must be provided.

    \b
    Examples:
        ldd-debug responses search run.db --text "error"
        ldd-debug responses search run.db --regex "case.*\\\\d{4}"
        ldd-debug responses search run.db --xpath "//div[@class='opinion']"
        ldd-debug responses search run.db --text "verdict" --format json
        ldd-debug responses search run.db --text "verdict" --format jsonl
    """
    # Validate exactly one search type is provided
    search_types = [text_pattern, regex_pattern, xpath_expr]
    provided = sum(1 for s in search_types if s is not None)
    if provided != 1:
        click.echo(
            "Error: Exactly one of --text, --regex, or --xpath must be provided",
            err=True,
        )
        sys.exit(1)

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            try:
                matches = await debugger.search_responses(
                    text=text_pattern,
                    regex=regex_pattern,
                    xpath=xpath_expr,
                    continuation=continuation,
                )

                output = {"items": matches}
                render_output(
                    output,
                    format_type=format_type,
                    template_path="responses/search",
                    template_name=template_name or "default",
                )

            except ValueError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)
            except Exception as e:
                click.echo(f"Search error: {e}", err=True)
                sys.exit(1)

    asyncio.run(run())
