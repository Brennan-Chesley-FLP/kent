"""CLI commands for inspecting and manipulating compression."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.dev_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Compression Commands
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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def compression_stats(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show compression statistics.

    \b
    Examples:
        ldd-debug compression stats run.db
        ldd-debug compression stats run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            stats = await debugger.get_compression_stats()

            if format_type == "table":
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
@click.argument("continuation")
@click.option(
    "--samples", default=1000, help="Number of samples to use for training"
)
@click.pass_context
def compression_train(
    ctx: click.Context, db_path: str | None, continuation: str, samples: int
) -> None:
    """Train a new compression dictionary for a continuation.

    \b
    Examples:
        ldd-debug compression train run.db step1
        ldd-debug compression train run.db step1 --samples 500
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                dict_id = await debugger.train_compression_dict(
                    continuation, sample_count=samples
                )
                click.echo(
                    f"Trained compression dictionary {dict_id} for {continuation}"
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
@click.argument("continuation")
@click.option(
    "--dict-id", type=int, help="Compression dictionary ID (default: latest)"
)
@click.pass_context
def compression_recompress(
    ctx: click.Context,
    db_path: str | None,
    continuation: str,
    dict_id: int | None,
) -> None:
    """Recompress responses with a compression dictionary.

    \b
    Examples:
        ldd-debug compression recompress run.db step1
        ldd-debug compression recompress run.db step1 --dict-id 5
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(
            db_path, read_only=False
        ) as debugger:
            try:
                stats = await debugger.recompress_responses(
                    continuation, dict_id
                )
                click.echo(f"Recompressed {stats['total']} responses")
                click.echo(f"Size before: {stats['size_before']} bytes")
                click.echo(f"Size after: {stats['size_after']} bytes")
                click.echo(f"Savings: {stats['savings']} bytes")
            except ValueError as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())
