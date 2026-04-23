"""CLI commands for scrape-level health checks and diagnostics."""

from __future__ import annotations

import asyncio

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    _run_health_report,
    register_cli_group,
)
from kent.driver.persistent_driver.cli._options import (
    db_option,
    format_options,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

scrape = register_cli_group(
    "scrape", "Scrape-level health checks and diagnostics."
)


@scrape.command("health")
@db_option
@format_options
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
    asyncio.run(
        _run_health_report(
            db_path, format_type, "scrape/health", template_name
        )
    )


@scrape.command("estimates")
@click.option(
    "--failures-only",
    is_flag=True,
    help="Only show failed estimates",
)
@db_option
@format_options
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
