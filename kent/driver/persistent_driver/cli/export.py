"""CLI commands for exporting results and responses."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Export Commands
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
def export(ctx: click.Context, db_path: str | None) -> None:
    """Export results and responses."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@export.command("jsonl")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("output_path", type=click.Path())
@click.option("--type", "result_type", help="Filter by result type")
@click.option(
    "--valid/--invalid", default=None, help="Filter by validation status"
)
@click.pass_context
def export_jsonl(
    ctx: click.Context,
    db_path: str | None,
    output_path: str,
    result_type: str | None,
    valid: bool | None,
) -> None:
    """Export results to JSONL (newline-delimited JSON) file.

    \b
    Examples:
        ldd-debug export jsonl run.db results.jsonl
        ldd-debug export jsonl run.db opinions.jsonl --type CourtOpinion --valid
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            count = await debugger.export_results_jsonl(
                output_path, result_type=result_type, is_valid=valid
            )
            click.echo(f"Exported {count} results to {output_path}")

    asyncio.run(run())


@export.command("warc")
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
@click.option("--continuation", help="Filter by continuation (step name)")
@click.pass_context
def export_warc(
    ctx: click.Context,
    db_path: str | None,
    output_path: str,
    compress: bool,
    continuation: str | None,
) -> None:
    """Export responses to WARC (Web ARChive) format.

    \b
    Examples:
        ldd-debug export warc run.db archive.warc.gz
        ldd-debug export warc run.db step1.warc --no-compress --continuation step1
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            try:
                count = await debugger.export_warc(
                    output_path, compress=compress, continuation=continuation
                )
                click.echo(f"Exported {count} responses to {output_path}")
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())
