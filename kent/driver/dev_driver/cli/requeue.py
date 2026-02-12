"""CLI commands for requeuing and cancelling requests."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.dev_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Requeue Commands
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
def requeue(ctx: click.Context, db_path: str | None) -> None:
    """Requeue requests or errors."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@requeue.command("request")
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
def requeue_request(
    ctx: click.Context,
    db_path: str | None,
    request_id: int,
    clear_downstream: bool,
) -> None:
    """Requeue a completed or failed request.

    \b
    Examples:
        ldd-debug requeue request run.db 123
        ldd-debug requeue request run.db 123 --no-clear-downstream
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


@requeue.command("continuation")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("continuation")
@click.option(
    "--status",
    type=click.Choice(["completed", "failed"]),
    default="completed",
    help="Which requests to requeue",
)
@click.pass_context
def requeue_continuation(
    ctx: click.Context, db_path: str | None, continuation: str, status: str
) -> None:
    """Requeue all requests for a continuation with a given status.

    \b
    Examples:
        ldd-debug requeue continuation run.db step1
        ldd-debug requeue continuation run.db step1 --status failed
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.requeue_continuation(
                continuation,
                status=status,  # type: ignore
            )
            click.echo(
                f"Requeued {count} {status} requests for {continuation}"
            )

    asyncio.run(run())


@requeue.command("errors")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--type", "error_type", help="Filter by error type")
@click.option("--continuation", help="Filter by continuation (step name)")
@click.pass_context
def requeue_errors(
    ctx: click.Context,
    db_path: str | None,
    error_type: str | None,
    continuation: str | None,
) -> None:
    """Batch requeue errors matching filter criteria.

    \b
    Examples:
        ldd-debug requeue errors run.db --type xpath
        ldd-debug requeue errors run.db --continuation step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.batch_requeue_errors(
                error_type=error_type, continuation=continuation
            )
            click.echo(f"Requeued {count} errors")

    asyncio.run(run())


# =========================================================================
# Cancel Commands
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
def cancel(ctx: click.Context, db_path: str | None) -> None:
    """Cancel pending or held requests."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@cancel.command("request")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("request_id", type=int)
@click.pass_context
def cancel_request(
    ctx: click.Context, db_path: str | None, request_id: int
) -> None:
    """Cancel a pending or held request.

    \b
    Examples:
        ldd-debug cancel request run.db 123
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


@cancel.command("continuation")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("continuation")
@click.pass_context
def cancel_continuation(
    ctx: click.Context, db_path: str | None, continuation: str
) -> None:
    """Cancel all pending/held requests for a continuation.

    \b
    Examples:
        ldd-debug cancel continuation run.db step1
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            count = await debugger.cancel_requests_by_continuation(
                continuation
            )
            click.echo(f"Cancelled {count} requests for {continuation}")

    asyncio.run(run())
