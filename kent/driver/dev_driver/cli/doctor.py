"""CLI commands for health checking and database diagnostics."""

from __future__ import annotations

import asyncio
import json

import click

from kent.driver.dev_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.dev_driver.debugger import LocalDevDriverDebugger

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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def doctor_health(
    ctx: click.Context, db_path: str | None, format_type: str
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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.pass_context
def doctor_orphans(
    ctx: click.Context, db_path: str | None, format_type: str
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
            orphans = await debugger.get_orphan_details()

            if format_type == "json":
                format_output(orphans, format_type)
            elif format_type == "jsonl":
                # Output orphaned requests
                for req in orphans["orphaned_requests"]:
                    click.echo(json.dumps({"type": "orphaned_request", **req}))
                # Output orphaned responses
                for resp in orphans["orphaned_responses"]:
                    click.echo(
                        json.dumps({"type": "orphaned_response", **resp})
                    )
            else:
                # Table output
                click.echo("=== Orphaned Requests ===")
                if orphans["orphaned_requests"]:
                    click.echo(f"Count: {len(orphans['orphaned_requests'])}")
                    headers = ["id", "url", "continuation", "completed_at"]
                    format_output(
                        orphans["orphaned_requests"],
                        "table",
                        headers,
                    )
                else:
                    click.echo("No orphaned requests found")

                click.echo("\n=== Orphaned Responses ===")
                if orphans["orphaned_responses"]:
                    click.echo(f"Count: {len(orphans['orphaned_responses'])}")
                    headers = ["id", "request_id", "url", "created_at"]
                    format_output(
                        orphans["orphaned_responses"],
                        "table",
                        headers,
                    )
                else:
                    click.echo("No orphaned responses found")

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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.option("--limit", default=100, help="Maximum number of results")
@click.pass_context
def doctor_pending(
    ctx: click.Context, db_path: str | None, format_type: str, limit: int
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

            if format_type == "table":
                click.echo(f"Total Pending: {page.total}")
                click.echo(f"Showing: {len(page.items)}")
                if page.items:
                    headers = [
                        "id",
                        "url",
                        "continuation",
                        "priority",
                        "retry_count",
                    ]
                    items = [
                        {
                            "id": r.id,
                            "url": r.url[:50] if r.url else "",
                            "continuation": r.continuation,
                            "priority": r.priority,
                            "retry_count": r.retry_count,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No pending requests found")
            else:
                output = {
                    "total": page.total,
                    "items": [
                        {
                            "id": r.id,
                            "url": r.url,
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
                format_output(output, format_type)

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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.option("--continuation", help="Filter by continuation (step name)")
@click.pass_context
def doctor_ghosts(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    continuation: str | None,
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
                    if format_type == "table":
                        click.echo(
                            f"No ghost requests found for continuation '{continuation}'"
                        )
                    else:
                        format_output(
                            {
                                "total_count": 0,
                                "by_continuation": {},
                                "ghosts": [],
                            },
                            format_type,
                        )
                    return

                # Filter ghosts to only include the specified continuation
                filtered_ghosts_list = [
                    g
                    for g in ghosts["ghosts"]
                    if g["continuation"] == continuation
                ]
                filtered_ghosts = {
                    "total_count": len(filtered_ghosts_list),
                    "by_continuation": {
                        continuation: ghosts["by_continuation"][continuation]
                    },
                    "ghosts": filtered_ghosts_list,
                }
                ghosts = filtered_ghosts

            if format_type == "json":
                format_output(ghosts, format_type)
            elif format_type == "jsonl":
                for ghost in ghosts["ghosts"]:
                    click.echo(json.dumps(ghost))
            else:
                # Table output
                click.echo("=== Ghost Requests ===")
                click.echo(f"Total: {ghosts['total_count']}")

                if ghosts["total_count"] > 0:
                    click.echo("\nBy Continuation:")
                    for cont, count in ghosts["by_continuation"].items():
                        click.echo(f"  {cont}: {count}")

                    if ghosts["ghosts"]:
                        click.echo("\nDetails:")
                        headers = ["id", "url", "continuation"]
                        items = [
                            {
                                "id": g["id"],
                                "url": g["url"][:50] if g.get("url") else "",
                                "continuation": g["continuation"],
                            }
                            for g in ghosts["ghosts"]
                        ]
                        format_output(items, "table", headers)
                else:
                    click.echo("No ghost requests found")

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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
    help="Output format",
)
@click.option(
    "--failures-only",
    is_flag=True,
    help="Only show failed estimates",
)
@click.pass_context
def doctor_estimates(
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
    type=click.Choice(["table", "json", "jsonl"]),
    default="table",
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
@click.pass_context
def doctor_structure(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    step_name: str | None,
    detailed: bool,
    request_id: int | None,
    response_id: int | None,
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

                if format_type in ("json", "jsonl"):
                    format_output(detail, format_type)
                else:
                    target = (
                        f"request {request_id}"
                        if request_id is not None
                        else f"response {response_id}"
                    )
                    click.echo(f"=== Validation Detail for {target} ===\n")
                    click.echo(f"  Step: {detail['continuation']}")
                    click.echo(
                        f"  Spec: {detail.get('spec_path', 'N/A')} ({detail.get('spec_type', 'N/A')})"
                    )
                    click.echo(f"  Status: {detail['status']}")
                    if detail.get("errors"):
                        click.echo("\n  Validation Errors:")
                        for err in detail["errors"]:
                            click.echo(f"    - {err}")
                    elif detail["status"] == "VALID":
                        click.echo("\n  No validation errors.")
                return

            # Summary / detailed mode
            result = await debugger.validate_structure(
                step_name=step_name,
            )

            if format_type == "json":
                format_output(result, format_type)
            elif format_type == "jsonl":
                for step in result["steps"]:
                    click.echo(json.dumps(step))
                click.echo(
                    json.dumps({"section": "summary", **result["summary"]})
                )
            else:
                click.echo("=== Structure Validation ===\n")

                if not result["steps"]:
                    click.echo("No steps with xsd or json_model specs found.")
                    return

                for step in result["steps"]:
                    cont = step["continuation"]
                    stype = step["spec_type"]
                    total = step["total_responses"]
                    valid = step["valid"]
                    invalid = step["invalid"]

                    click.echo(f"{cont} ({stype}):")
                    click.echo(
                        f"  Total: {total}  Valid: {valid}  Invalid: {invalid}"
                    )

                    if detailed and invalid > 0:
                        req_ids = step["invalid_request_ids"]
                        resp_ids = step["invalid_response_ids"]
                        # Pair them up (they correspond by index)
                        for i, rid in enumerate(req_ids):
                            resp = resp_ids[i] if i < len(resp_ids) else "?"
                            click.echo(
                                f"    request_id={rid}  response_id={resp}"
                            )
                    click.echo()

                # Summary
                s = result["summary"]
                click.echo(
                    f"Summary: {s['total_responses_checked']} responses checked, "
                    f"{s['total_valid']} valid, {s['total_invalid']} invalid"
                )

    asyncio.run(run())
