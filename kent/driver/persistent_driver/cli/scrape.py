"""CLI commands for scrape-level health checks and diagnostics."""

from __future__ import annotations

import asyncio
import json

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def scrape_health(
    ctx: click.Context, db_path: str | None, format_type: str
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

            if format_type == "json":
                # JSON output
                output = {
                    "status": status,
                    "integrity": integrity,
                    "ghosts": ghosts,
                    "error_stats": stats["errors"],
                    "estimates": estimates,
                }
                format_output(output, format_type)
            elif format_type == "jsonl":
                # JSONL output (one line per section)
                click.echo(json.dumps({"section": "status", **status}))
                click.echo(json.dumps({"section": "integrity", **integrity}))
                click.echo(json.dumps({"section": "ghosts", **ghosts}))
                click.echo(
                    json.dumps({"section": "errors", **stats["errors"]})
                )
                click.echo(json.dumps({"section": "estimates", **estimates}))
            else:
                # Table output (default)
                click.echo("=== Health Report ===\n")

                # Run Status
                click.echo("Run Status:")
                click.echo(f"  Status: {status['status']}")
                if status.get("is_running"):
                    click.echo(
                        f"  Pending Requests: {status['pending_count']}"
                    )
                click.echo()

                # Integrity Check Summary
                click.echo("Integrity Check:")
                if integrity["has_issues"]:
                    click.echo(
                        f"  Orphaned Requests: {integrity['orphaned_requests']['count']}"
                    )
                    click.echo(
                        f"  Orphaned Responses: {integrity['orphaned_responses']['count']}"
                    )
                else:
                    click.echo("  No integrity issues found")
                click.echo()

                # Error Summary
                click.echo("Errors:")
                click.echo(f"  Total: {stats['errors']['total']}")
                click.echo(f"  Unresolved: {stats['errors']['unresolved']}")
                click.echo()

                # Ghost Request Summary
                click.echo("Ghost Requests:")
                if ghosts["total_count"] > 0:
                    click.echo(f"  Total: {ghosts['total_count']}")
                    click.echo("  By Continuation:")
                    for continuation, count in ghosts[
                        "by_continuation"
                    ].items():
                        click.echo(f"    {continuation}: {count}")
                else:
                    click.echo("  No ghost requests found")
                click.echo()

                # Estimate Check Summary
                click.echo("Estimates:")
                est_summary = estimates["summary"]
                if est_summary["total"] > 0:
                    click.echo(
                        f"  Total: {est_summary['total']}  "
                        f"Passed: {est_summary['passed']}  "
                        f"Failed: {est_summary['failed']}"
                    )
                else:
                    click.echo("  No estimates recorded")

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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.option(
    "--failures-only",
    is_flag=True,
    help="Only show failed estimates",
)
@click.pass_context
def scrape_estimates(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    failures_only: bool,
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

            if format_type == "json":
                format_output(result, format_type)
            elif format_type == "jsonl":
                for est in estimates:
                    click.echo(json.dumps(est))
                click.echo(
                    json.dumps({"section": "summary", **result["summary"]})
                )
            else:
                click.echo("=== Estimate Checks ===\n")

                if not estimates:
                    if failures_only:
                        click.echo("No failed estimates.")
                    else:
                        click.echo("No estimates recorded.")
                    return

                for est in estimates:
                    types_str = ", ".join(est["expected_types"])
                    max_str = (
                        str(est["max_count"])
                        if est["max_count"] is not None
                        else "unbounded"
                    )
                    status_marker = (
                        "PASS" if est["status"] == "pass" else "FAIL"
                    )
                    click.echo(
                        f"  [{status_marker}] request_id={est['request_id']} "
                        f"types=[{types_str}] "
                        f"expected={est['min_count']}-{max_str} "
                        f"actual={est['actual_count']}"
                    )

                click.echo()
                s = result["summary"]
                click.echo(
                    f"Summary: {s['total']} estimates, "
                    f"{s['passed']} passed, {s['failed']} failed"
                )

    asyncio.run(run())
