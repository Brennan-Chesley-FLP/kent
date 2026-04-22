"""CLI commands for cancelling requests."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli._options import db_option
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger


@cli.group()
@db_option
@click.pass_context
def cancel(ctx: click.Context, db_path: str | None) -> None:
    """Cancel pending or held requests."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@cancel.command("request")
@click.argument("request_id", type=int)
@db_option
@click.pass_context
def cancel_request(
    ctx: click.Context, db_path: str | None, request_id: int
) -> None:
    """Cancel a pending or held request.

    \b
    Examples:
        pdd cancel request --db run.db 123
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
@click.argument("continuation")
@db_option
@click.pass_context
def cancel_continuation(
    ctx: click.Context, db_path: str | None, continuation: str
) -> None:
    """Cancel all pending/held requests for a continuation.

    \b
    Examples:
        pdd cancel continuation --db run.db step1
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
