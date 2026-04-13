"""CLI commands for inspecting and manipulating requests."""

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
from kent.driver.persistent_driver.cli.templating import render_output
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
@click.option("--step", help="Filter by step name")
@click.option("--limit", default=100, help="Maximum number of results")
@click.option("--offset", default=0, help="Number of results to skip")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_list(
    ctx: click.Context,
    db_path: str | None,
    status: str | None,
    step: str | None,
    limit: int,
    offset: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """List requests with optional filtering.

    \b
    Examples:
        pdd requests list --db run.db
        pdd requests list --db run.db --status failed
        pdd requests list --db run.db --step step1 --limit 50
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(
                status=status,  # type: ignore
                continuation=step,
                limit=limit,
                offset=offset,
            )

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
            render_output(
                output,
                format_type=format_type,
                template_path="requests/list",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_show(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show detailed request information.

    \b
    Examples:
        pdd requests show --db run.db 123
        pdd requests show --db run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            request = await debugger.get_request(request_id)

            if request is None:
                click.echo(f"Request {request_id} not found", err=True)
                import sys

                sys.exit(1)

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
            render_output(
                output,
                format_type=format_type,
                template_path="requests/show",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@requests.command("parents")
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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_parents(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show the chain of parent requests from a request to the root.

    Walks up the parent_request_id links to show the full ancestry
    of a request, from the given request to the entry-point request.

    \b
    Examples:
        pdd requests parents --db run.db 456
        pdd requests parents --db run.db 456 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            chain = await debugger.get_parent_chain(request_id)

            if not chain:
                click.echo(f"Request {request_id} not found", err=True)
                sys.exit(1)

            output = {
                "request_id": request_id,
                "items": chain,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="requests/parents",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_summary(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show request counts by status and continuation.

    \b
    Examples:
        pdd requests summary --db run.db
        pdd requests summary --db run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_request_summary()

            output = {
                "items": summary,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="requests/summary",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@requests.command("content")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("request_id", type=int)
@click.option("--output", "-o", help="Output file path (default: stdout)")
@click.pass_context
def requests_content(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    output: str | None,
) -> None:
    """Get decompressed response content for a request.

    \b
    Examples:
        pdd requests content --db run.db 123
        pdd requests content --db run.db 123 -o response.html
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


@requests.command("search")
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
@click.option("--step", help="Filter by step name")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_search(
    ctx: click.Context,
    db_path: str | None,
    text_pattern: str | None,
    regex_pattern: str | None,
    xpath_expr: str | None,
    step: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """Search response content for matching patterns.

    Searches through all response content (decompressed) for matches.
    Exactly one of --text, --regex, or --xpath must be provided.

    \b
    Examples:
        pdd requests search --db run.db --text "error"
        pdd requests search --db run.db --regex "case.*\\\\d{4}"
        pdd requests search --db run.db --xpath "//div[@class='opinion']"
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
                    continuation=step,
                )

                output = {
                    "items": matches,
                }
                render_output(
                    output,
                    format_type=format_type,
                    template_path="requests/search",
                    template_name=template_name or "default",
                )

            except ValueError as e:
                click.echo(f"Error: {e}", err=True)
                sys.exit(1)
            except Exception as e:
                click.echo(f"Search error: {e}", err=True)
                sys.exit(1)

    asyncio.run(run())


@requests.command("cancel")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("request_id", type=int)
@click.pass_context
def requests_cancel(
    ctx: click.Context, db_path: str | None, request_id: int
) -> None:
    """Cancel a pending or held request.

    \b
    Examples:
        pdd requests cancel --db run.db 123
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            cancelled = await debugger.cancel_request(request_id)

            if cancelled:
                click.echo(f"Request {request_id} cancelled")
            else:
                click.echo(
                    f"Request {request_id} not found or not pending/held",
                    err=True,
                )
                sys.exit(1)

    asyncio.run(run())


@requests.command("cancel-all")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step")
@click.pass_context
def requests_cancel_all(
    ctx: click.Context, db_path: str | None, step: str
) -> None:
    """Cancel all pending/held requests for a step.

    \b
    Examples:
        pdd requests cancel-all --db run.db step1
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.cancel_requests_by_continuation(step)
            click.echo(f"Cancelled {count} requests for {step}")

    asyncio.run(run())


@requests.command("export")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("output_path", type=click.Path())
@click.option(
    "--compress/--no-compress",
    default=True,
    help="Gzip-compress the WARC file",
)
@click.option("--step", help="Filter by step name")
@click.pass_context
def requests_export(
    ctx: click.Context,
    db_path: str | None,
    output_path: str,
    compress: bool,
    step: str | None,
) -> None:
    """Export responses to WARC (Web ARChive) format.

    \b
    Examples:
        pdd requests export --db run.db archive.warc.gz
        pdd requests export --db run.db step1.warc --no-compress --step step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            try:
                count = await debugger.export_warc(
                    output_path, compress=compress, continuation=step
                )
                click.echo(f"Exported {count} responses to {output_path}")
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())


# =========================================================================
# Compression Subgroup
# =========================================================================


@requests.group()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.pass_context
def compression(ctx: click.Context, db_path: str | None) -> None:
    """Inspect and manipulate compression."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@compression.command("stats")
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
def compression_stats(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show compression statistics.

    \b
    Examples:
        pdd requests compression stats --db run.db
        pdd requests compression stats --db run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            stats = await debugger.get_compression_stats()

            if format_type == "summary":
                click.echo("=== Compression Statistics ===")
                click.echo(f"Total Responses: {stats['total']}")
                click.echo(
                    f"Total Original Size: {stats['total_original']} bytes"
                )
                click.echo(
                    f"Total Compressed Size: {stats['total_compressed']} bytes"
                )
                click.echo(f"With Dict: {stats['with_dict']}")
                click.echo(f"No Dict: {stats['no_dict']}")
                click.echo(
                    f"Compression Ratio: {stats['compression_ratio']:.2f}x"
                )
            else:
                format_output(stats, format_type)

    asyncio.run(run())


@compression.command("train")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step")
@click.option(
    "--sample", default=1000, help="Number of samples to use for training"
)
@click.pass_context
def compression_train(
    ctx: click.Context, db_path: str | None, step: str, sample: int
) -> None:
    """Train a new compression dictionary for a step.

    \b
    Examples:
        pdd requests compression train --db run.db step1
        pdd requests compression train --db run.db step1 --sample 500
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                dict_id = await debugger.train_compression_dict(
                    step, sample_count=sample
                )
                click.echo(
                    f"Trained compression dictionary {dict_id} for {step}"
                )
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())


@compression.command("recompress")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step")
@click.option(
    "--dict-id", type=int, help="Compression dictionary ID (default: latest)"
)
@click.pass_context
def compression_recompress(
    ctx: click.Context,
    db_path: str | None,
    step: str,
    dict_id: int | None,
) -> None:
    """Recompress responses with a compression dictionary.

    \b
    Examples:
        pdd requests compression recompress --db run.db step1
        pdd requests compression recompress --db run.db step1 --dict-id 5
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                stats = await debugger.recompress_responses(step, dict_id)
                click.echo(f"Recompressed {stats['total']} responses")
                click.echo(f"Size before: {stats['size_before']} bytes")
                click.echo(f"Size after: {stats['size_after']} bytes")
                click.echo(f"Savings: {stats['savings']} bytes")
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())


@requests.command("pending")
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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option("--limit", default=100, help="Maximum number of results")
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_pending(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    limit: int,
    template_name: str | None,
) -> None:
    """List pending requests with details.

    \b
    Examples:
        pdd requests pending --db run.db
        pdd requests --db run.db pending --limit 50
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(
                status="pending", limit=limit, offset=0
            )

            output = {
                "total": page.total,
                "items": [
                    {
                        "id": r.id,
                        "url": r.url,
                        "continuation": r.continuation,
                        "priority": r.priority,
                        "retry_count": r.retry_count,
                        "method": r.method,
                        "created_at": r.created_at,
                    }
                    for r in page.items
                ],
                "limit": limit,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="requests/pending",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@requests.command("ghosts")
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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option("--step", help="Filter by step name")
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_ghosts(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    step: str | None,
    template_name: str | None,
) -> None:
    """List ghost requests grouped by step.

    Ghost requests are completed requests with no child requests and no results.

    \b
    Examples:
        pdd requests ghosts --db run.db
        pdd requests --db run.db ghosts --step parse_index
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            ghosts = await debugger.get_ghost_requests()

            # Filter by step if specified
            if step:
                if step not in ghosts["by_continuation"]:
                    output = {
                        "total_count": 0,
                        "by_continuation": {},
                        "items": [],
                    }
                    render_output(
                        output,
                        format_type=format_type,
                        template_path="requests/ghosts",
                        template_name=template_name or "default",
                    )
                    return

                # Filter ghosts to only include the specified step
                filtered_ghosts_list = [
                    g for g in ghosts["ghosts"] if g["continuation"] == step
                ]
                ghosts = {
                    "total_count": len(filtered_ghosts_list),
                    "by_continuation": {step: ghosts["by_continuation"][step]},
                    "ghosts": filtered_ghosts_list,
                }

            output = {
                "total_count": ghosts["total_count"],
                "by_continuation": ghosts["by_continuation"],
                "items": ghosts["ghosts"],
            }
            render_output(
                output,
                format_type=format_type,
                template_path="requests/ghosts",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@requests.command("orphans")
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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def requests_orphans(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """List orphaned requests and responses with details.

    \b
    Examples:
        pdd requests orphans --db run.db
        pdd requests --db run.db orphans --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            orphans = await debugger.get_orphan_details()

            # Build items list for jsonl: each entry tagged with type
            items = [
                {"type": "orphaned_request", **req}
                for req in orphans["orphaned_requests"]
            ] + [
                {"type": "orphaned_response", **resp}
                for resp in orphans["orphaned_responses"]
            ]
            output = {
                "orphaned_requests": orphans["orphaned_requests"],
                "orphaned_responses": orphans["orphaned_responses"],
                "items": items,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="requests/orphans",
                template_name=template_name or "default",
            )

    asyncio.run(run())
