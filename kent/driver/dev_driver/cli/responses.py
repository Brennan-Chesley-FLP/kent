"""CLI commands for inspecting responses."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path

import click

from kent.driver.dev_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Responses Commands
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
def responses(ctx: click.Context, db_path: str | None) -> None:
    """Inspect responses."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@responses.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
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
def responses_list(
    ctx: click.Context,
    db_path: str | None,
    continuation: str | None,
    limit: int,
    offset: int,
    format_type: str,
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

            if format_type == "table":
                click.echo(
                    f"Total: {page.total}, Showing: {len(page.items)}, "
                    f"Offset: {offset}, Limit: {limit}"
                )
                if page.items:
                    headers = [
                        "id",
                        "status_code",
                        "url",
                        "continuation",
                        "size",
                    ]
                    items = [
                        {
                            "id": r.id,
                            "status_code": r.status_code,
                            "url": r.url[:50] if r.url else "",
                            "continuation": r.continuation,
                            "size": r.content_size_original,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No responses found")
            else:
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
                format_output(output, format_type)

    asyncio.run(run())


@responses.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("response_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def responses_show(
    ctx: click.Context, db_path: str | None, response_id: int, format_type: str
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
            response = await debugger.get_response(response_id)

            if response is None:
                click.echo(f"Response {response_id} not found", err=True)
                sys.exit(1)

            if format_type == "table":
                click.echo(f"ID: {response.id}")
                click.echo(f"Request ID: {response.request_id}")
                click.echo(f"Status Code: {response.status_code}")
                click.echo(f"URL: {response.url}")
                click.echo(f"Continuation: {response.continuation}")
                click.echo(f"Original Size: {response.content_size_original}")
                click.echo(
                    f"Compressed Size: {response.content_size_compressed}"
                )
                click.echo(
                    f"Compression Ratio: {response.compression_ratio:.2f}x"
                )
                click.echo(f"Created At: {response.created_at}")
            else:
                output = {
                    "id": response.id,
                    "request_id": response.request_id,
                    "status_code": response.status_code,
                    "url": response.url,
                    "continuation": response.continuation,
                    "content_size_original": response.content_size_original,
                    "content_size_compressed": response.content_size_compressed,
                    "compression_ratio": response.compression_ratio,
                    "created_at": response.created_at,
                }
                format_output(output, format_type)

    asyncio.run(run())


@responses.command("content")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("response_id", type=int)
@click.option("--output", "-o", help="Output file path (default: stdout)")
@click.pass_context
def responses_content(
    ctx: click.Context,
    db_path: str | None,
    response_id: int,
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
            content = await debugger.get_response_content(response_id)

            if content is None:
                click.echo(f"Response {response_id} not found", err=True)
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
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--text", "text_pattern", help="Plain text to search for")
@click.option("--regex", "regex_pattern", help="Regular expression pattern")
@click.option("--xpath", "xpath_expr", help="XPath expression to evaluate")
@click.option("--continuation", help="Filter by continuation (step name)")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def responses_search(
    ctx: click.Context,
    db_path: str | None,
    text_pattern: str | None,
    regex_pattern: str | None,
    xpath_expr: str | None,
    continuation: str | None,
    format_type: str,
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

                if format_type == "table":
                    if matches:
                        click.echo(f"Found {len(matches)} matching responses:")
                        for match in matches:
                            click.echo(
                                f"  response_id={match['response_id']}, "
                                f"request_id={match['request_id']}"
                            )
                    else:
                        click.echo("No matching responses found")
                elif format_type == "json":
                    click.echo(json.dumps(matches, indent=2))
                elif format_type == "jsonl":
                    for match in matches:
                        click.echo(json.dumps(match))

            except ValueError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)
            except Exception as e:
                click.echo(f"Search error: {e}", err=True)
                sys.exit(1)

    asyncio.run(run())
