"""CLI commands for inspecting and manipulating requests."""

from __future__ import annotations

import asyncio
import json
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
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

            if format_type == "summary":
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def requests_show(
    ctx: click.Context, db_path: str | None, request_id: int, format_type: str
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

            if format_type == "summary":
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def requests_parents(
    ctx: click.Context, db_path: str | None, request_id: int, format_type: str
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

            if format_type == "summary":
                click.echo(
                    f"Ancestry for request {request_id} "
                    f"({len(chain)} levels):\n"
                )
                for entry in chain:
                    depth = entry["depth"]
                    indent = "  " * depth
                    marker = "*" if depth == 0 else " "
                    parent_info = (
                        f" (parent: {entry['parent_request_id']})"
                        if entry["parent_request_id"] is not None
                        else " (root)"
                    )
                    click.echo(
                        f"{indent}{marker} [{entry['id']}] "
                        f"{entry['continuation']} "
                        f"[{entry['status']}]{parent_info}"
                    )
                    click.echo(f"{indent}  {entry['url']}")
            else:
                format_output(chain, format_type)

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def requests_summary(
    ctx: click.Context, db_path: str | None, format_type: str
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

            if format_type == "summary":
                for continuation, status_counts in summary.items():
                    click.echo(f"\n=== {continuation} ===")
                    for status, count in status_counts.items():
                        click.echo(f"  {status}: {count}")
            else:
                format_output(summary, format_type)

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
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

                if format_type == "summary":
                    if matches:
                        click.echo(f"Found {len(matches)} matching responses:")
                        for match in matches:
                            click.echo(f"  request_id={match['request_id']}")
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


@requests.command("requeue")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("request_id", type=int)
@click.option(
    "--clear-downstream/--no-clear-downstream",
    default=True,
    help="Clear downstream data (responses, results, errors)",
)
@click.pass_context
def requests_requeue(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    clear_downstream: bool,
) -> None:
    """Requeue a completed or failed request.

    \b
    Examples:
        pdd requests requeue --db run.db 123
        pdd requests requeue --db run.db 123 --no-clear-downstream
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                new_id = await debugger.requeue_request(
                    request_id, clear_downstream=clear_downstream
                )
                click.echo(
                    f"Request {request_id} requeued as request {new_id}"
                )
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())


@requests.command("requeue-all")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step")
@click.option(
    "--status",
    type=click.Choice(["completed", "failed"]),
    default="completed",
    help="Which requests to requeue",
)
@click.pass_context
def requests_requeue_all(
    ctx: click.Context, db_path: str | None, step: str, status: str
) -> None:
    """Requeue all requests for a step with a given status.

    \b
    Examples:
        pdd requests requeue-all --db run.db step1
        pdd requests requeue-all --db run.db step1 --status failed
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.requeue_continuation(
                step,
                status=status,  # type: ignore
            )
            click.echo(f"Requeued {count} {status} requests for {step}")

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_context
def requests_pending(
    ctx: click.Context, db_path: str | None, format_type: str, limit: int
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

            if format_type == "summary":
                click.echo(f"Total Pending: {page.total}")
                click.echo(f"Showing: {len(page.items)}")
                if page.items:
                    headers = [
                        "id",
                        "url",
                        "continuation",
                        "priority",
                        "retry_count",
                    ]
                    items = [
                        {
                            "id": r.id,
                            "url": r.url[:50] if r.url else "",
                            "continuation": r.continuation,
                            "priority": r.priority,
                            "retry_count": r.retry_count,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No pending requests found")
            else:
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
                format_output(output, format_type)

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.option("--step", help="Filter by step name")
@click.pass_context
def requests_ghosts(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    step: str | None,
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
                    if format_type == "summary":
                        click.echo(
                            f"No ghost requests found for step '{step}'"
                        )
                    else:
                        format_output(
                            {
                                "total_count": 0,
                                "by_continuation": {},
                                "ghosts": [],
                            },
                            format_type,
                        )
                    return

                # Filter ghosts to only include the specified step
                filtered_ghosts_list = [
                    g for g in ghosts["ghosts"] if g["continuation"] == step
                ]
                filtered_ghosts = {
                    "total_count": len(filtered_ghosts_list),
                    "by_continuation": {step: ghosts["by_continuation"][step]},
                    "ghosts": filtered_ghosts_list,
                }
                ghosts = filtered_ghosts

            if format_type == "json":
                format_output(ghosts, format_type)
            elif format_type == "jsonl":
                for ghost in ghosts["ghosts"]:
                    click.echo(json.dumps(ghost))
            else:
                # Table output
                click.echo("=== Ghost Requests ===")
                click.echo(f"Total: {ghosts['total_count']}")

                if ghosts["total_count"] > 0:
                    click.echo("\nBy Continuation:")
                    for cont, count in ghosts["by_continuation"].items():
                        click.echo(f"  {cont}: {count}")

                    if ghosts["ghosts"]:
                        click.echo("\nDetails:")
                        headers = ["id", "url", "continuation"]
                        items = [
                            {
                                "id": g["id"],
                                "url": g["url"][:50] if g.get("url") else "",
                                "continuation": g["continuation"],
                            }
                            for g in ghosts["ghosts"]
                        ]
                        format_output(items, "summary", headers)
                else:
                    click.echo("No ghost requests found")

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def requests_orphans(
    ctx: click.Context, db_path: str | None, format_type: str
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

            if format_type == "json":
                format_output(orphans, format_type)
            elif format_type == "jsonl":
                # Output orphaned requests
                for req in orphans["orphaned_requests"]:
                    click.echo(json.dumps({"type": "orphaned_request", **req}))
                # Output orphaned responses
                for resp in orphans["orphaned_responses"]:
                    click.echo(
                        json.dumps({"type": "orphaned_response", **resp})
                    )
            else:
                # Table output
                click.echo("=== Orphaned Requests ===")
                if orphans["orphaned_requests"]:
                    click.echo(f"Count: {len(orphans['orphaned_requests'])}")
                    headers = ["id", "url", "continuation", "completed_at"]
                    format_output(
                        orphans["orphaned_requests"],
                        "summary",
                        headers,
                    )
                else:
                    click.echo("No orphaned requests found")

                click.echo("\n=== Orphaned Responses ===")
                if orphans["orphaned_responses"]:
                    click.echo(f"Count: {len(orphans['orphaned_responses'])}")
                    headers = ["id", "request_id", "url", "created_at"]
                    format_output(
                        orphans["orphaned_responses"],
                        "summary",
                        headers,
                    )
                else:
                    click.echo("No orphaned responses found")

    asyncio.run(run())
