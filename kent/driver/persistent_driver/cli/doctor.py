"""CLI commands for health checking and database diagnostics."""

from __future__ import annotations

import asyncio

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Doctor Commands
# =========================================================================


@cli.group(invoke_without_command=True)
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.pass_context
def doctor(ctx: click.Context, db_path: str | None) -> None:
    """Run health checks on database.

    \b
    Examples:
        ldd-debug doctor --db run.db health
        ldd-debug doctor health --db run.db
        ldd-debug doctor structure --db run.db
        ldd-debug doctor structure --db run.db --detailed
    """
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path
    if ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())


@doctor.command("health")
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
def doctor_health(
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
        ldd-debug doctor health --db run.db
        ldd-debug doctor --db run.db health
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
                template_path="doctor/health",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@doctor.command("orphans")
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
def doctor_orphans(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
) -> None:
    """List orphaned requests and responses with details.

    \b
    Examples:
        ldd-debug doctor orphans --db run.db
        ldd-debug doctor --db run.db orphans --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            output = await debugger.get_orphan_details()

            render_output(
                output,
                format_type=format_type,
                template_path="doctor/orphans",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@doctor.command("pending")
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
@click.option("--limit", default=100, help="Maximum number of results")
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def doctor_pending(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    limit: int,
    template_name: str | None,
) -> None:
    """List pending requests with details.

    \b
    Examples:
        ldd-debug doctor pending --db run.db
        ldd-debug doctor --db run.db pending --limit 50
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_requests(
                status="pending", limit=limit, offset=0
            )

            output = {
                "total": page.total,
                "items": [
                    {
                        "id": r.id,
                        "url": r.url[:50] if r.url else "",
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
            render_output(
                output,
                format_type=format_type,
                template_path="doctor/pending",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@doctor.command("ghosts")
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
@click.option("--continuation", help="Filter by continuation (step name)")
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def doctor_ghosts(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    continuation: str | None,
    template_name: str | None,
) -> None:
    """List ghost requests grouped by step.

    Ghost requests are completed requests with no child requests and no results.

    \b
    Examples:
        ldd-debug doctor ghosts --db run.db
        ldd-debug doctor --db run.db ghosts --continuation parse_index
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            ghosts = await debugger.get_ghost_requests()

            # Filter by continuation if specified
            if continuation:
                if continuation not in ghosts["by_continuation"]:
                    output = {
                        "total_count": 0,
                        "by_continuation": {},
                        "ghosts": [],
                    }
                    render_output(
                        output,
                        format_type=format_type,
                        template_path="doctor/ghosts",
                        template_name=template_name or "default",
                    )
                    return

                # Filter ghosts to only include the specified continuation
                filtered_ghosts_list = [
                    g
                    for g in ghosts["ghosts"]
                    if g["continuation"] == continuation
                ]
                ghosts = {
                    "total_count": len(filtered_ghosts_list),
                    "by_continuation": {
                        continuation: ghosts["by_continuation"][continuation]
                    },
                    "ghosts": filtered_ghosts_list,
                }

            # Truncate URLs for display in template
            ghosts["ghosts"] = [
                {
                    **g,
                    "url": g["url"][:50] if g.get("url") else "",
                }
                for g in ghosts["ghosts"]
            ]

            render_output(
                ghosts,
                format_type=format_type,
                template_path="doctor/ghosts",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@doctor.command("estimates")
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
def doctor_estimates(
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
        ldd-debug doctor estimates --db run.db
        ldd-debug doctor --db run.db estimates --failures-only
        ldd-debug doctor estimates --db run.db --format json
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
                template_path="doctor/estimates",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@doctor.command("structure")
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
    "--step", "step_name", default=None, help="Filter to a specific step name"
)
@click.option(
    "--detailed",
    is_flag=True,
    help="Show request_id/response_id pairs for failures",
)
@click.option(
    "--request",
    "request_id",
    type=int,
    default=None,
    help="Show detailed validation for a specific request",
)
@click.option(
    "--response",
    "response_id",
    type=int,
    default=None,
    help="Show detailed validation for a specific response",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def doctor_structure(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    step_name: str | None,
    detailed: bool,
    request_id: int | None,
    response_id: int | None,
    template_name: str | None,
) -> None:
    """Validate stored responses against step XSD/JSON model specs.

    Finds all steps with xsd or json_model annotations and validates
    stored responses against those specs.

    \b
    Default mode shows pass/fail statistics by continuation.
    --step filters to a single step.
    --detailed shows request_id/response_id pairs for failures.
    --request or --response shows full validation error detail.

    \b
    Examples:
        ldd-debug doctor structure --db run.db
        ldd-debug doctor --db run.db structure --step parse_opinions_page
        ldd-debug doctor structure --db run.db --detailed
        ldd-debug doctor structure --db run.db --request 15
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Single request/response detail mode
            if request_id is not None or response_id is not None:
                detail = await debugger.validate_structure_detail(
                    request_id=request_id,
                    response_id=response_id,
                )

                target = (
                    f"request {request_id}"
                    if request_id is not None
                    else f"response {response_id}"
                )
                output = {
                    "detail": detail,
                    "target": target,
                }
                render_output(
                    output,
                    format_type=format_type,
                    template_path="doctor/structure",
                    template_name=template_name or "default",
                )
                return

            # Summary / detailed mode
            result = await debugger.validate_structure(
                step_name=step_name,
            )

            output = {
                "detail": None,
                "steps": result["steps"],
                "summary": result["summary"],
                "detailed": detailed,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="doctor/structure",
                template_name=template_name or "default",
            )

    asyncio.run(run())
