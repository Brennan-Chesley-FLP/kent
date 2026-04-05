"""CLI commands for scrape-level health checks and diagnostics."""

from __future__ import annotations

import asyncio

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger


@cli.group()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.pass_context
def scrape(ctx: click.Context, db_path: str | None) -> None:
    """Scrape-level health checks and diagnostics."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@scrape.command("health")
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
    type=click.Choice(["default", "summary", "table", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def scrape_health(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """Show comprehensive health report.

    Displays integrity check summary, error counts, pending/wrapped status,
    and ghost request summary by step.

    \b
    Examples:
        pdd scrape health --db run.db
        pdd scrape --db run.db health
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Get all health check data
            integrity = await debugger.check_integrity()
            ghosts = await debugger.get_ghost_requests()
            status = await debugger.get_run_status()
            stats = await debugger.get_stats()
            estimates = await debugger.check_estimates()

            output = {
                "status": status,
                "integrity": integrity,
                "ghosts": ghosts,
                "error_stats": stats["errors"],
                "estimates": estimates,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="scrape/health",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@scrape.command("estimates")
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
    type=click.Choice(["default", "summary", "table", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--failures-only",
    is_flag=True,
    help="Only show failed estimates",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def scrape_estimates(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    failures_only: bool,
    template_name: str | None,
) -> None:
    """Check EstimateData predictions against actual result counts.

    Verifies that the actual number of results produced by downstream
    requests matches the estimates declared by scraper steps.

    \b
    Examples:
        pdd scrape estimates --db run.db
        pdd scrape --db run.db estimates --failures-only
        pdd scrape estimates --db run.db --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.check_estimates()

            estimates = result["estimates"]
            if failures_only:
                estimates = [e for e in estimates if e["status"] == "fail"]

            output = {
                "items": estimates,
                "summary": result["summary"],
                "failures_only": failures_only,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="scrape/estimates",
                template_name=template_name or "default",
            )

    asyncio.run(run())
