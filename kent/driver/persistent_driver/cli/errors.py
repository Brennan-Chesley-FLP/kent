"""CLI commands for inspecting and manipulating errors."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    register_cli_group,
)
from kent.driver.persistent_driver.cli._options import (
    db_option,
    format_options,
    pagination_options,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Errors Commands
# =========================================================================


errors = register_cli_group("errors", "Inspect and manipulate errors.")


@errors.command("list")
@click.option("--type", "error_type", help="Filter by error type")
@click.option(
    "--resolved/--unresolved",
    default=None,
    help="Filter by resolution status",
)
@click.option("--step", help="Filter by step name")
@db_option
@format_options
@pagination_options
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
@click.argument("error_id", type=int)
@db_option
@format_options
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
@db_option
@format_options
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
@click.argument("error_id", type=int)
@click.option("--notes", help="Resolution notes")
@db_option
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


# =========================================================================
# Diagnose Command (moved from compare.py)
# =========================================================================


@errors.command("diagnose")
@click.argument("error_id", type=int)
@db_option
@format_options
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
