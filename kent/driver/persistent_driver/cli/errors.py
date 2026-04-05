"""CLI commands for inspecting and manipulating errors."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli.templating import render_output
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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
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
    template_name: str | None,
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

            output = {
                "total": page.total,
                "items": page.items,
                "limit": limit,
                "offset": offset,
                "has_more": page.has_more,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="errors/list",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def errors_show(
    ctx: click.Context,
    db_path: str | None,
    error_id: int,
    format_type: str,
    template_name: str | None,
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

            render_output(
                error,
                format_type=format_type,
                template_path="errors/show",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def errors_summary(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
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

            render_output(
                summary,
                format_type=format_type,
                template_path="errors/summary",
                template_name=template_name or "default",
            )

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
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def errors_diagnose(
    ctx: click.Context,
    db_path: str | None,
    error_id: int,
    format_type: str,
    template_name: str | None,
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

                render_output(
                    result,
                    format_type=format_type,
                    template_path="errors/diagnose",
                    template_name=template_name or "default",
                )
            except (ValueError, ImportError) as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())
