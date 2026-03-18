"""CLI commands for inspecting and exporting results."""

from __future__ import annotations

import asyncio
import json
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
    format_output,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Results Commands
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
def results(ctx: click.Context, db_path: str | None) -> None:
    """Inspect and export results."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


@results.command("list")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.option("--type", "result_type", help="Filter by result type")
@click.option(
    "--valid/--invalid", default=None, help="Filter by validation status"
)
@click.option("--limit", default=100, help="Maximum number of results")
@click.option("--offset", default=0, help="Number of results to skip")
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def results_list(
    ctx: click.Context,
    db_path: str | None,
    result_type: str | None,
    valid: bool | None,
    limit: int,
    offset: int,
    format_type: str,
) -> None:
    """List results with optional filtering.

    \b
    Examples:
        pdd --db run.db results list
        pdd --db run.db results list --type CourtOpinion --valid
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            page = await debugger.list_results(
                result_type=result_type,
                is_valid=valid,
                limit=limit,
                offset=offset,
            )

            if format_type == "summary":
                click.echo(
                    f"Total: {page.total}, Showing: {len(page.items)}, "
                    f"Offset: {offset}, Limit: {limit}"
                )
                if page.items:
                    headers = ["id", "type", "valid", "request_id"]
                    items = [
                        {
                            "id": r.id,
                            "type": r.result_type,
                            "valid": "\u2713" if r.is_valid else "\u2717",
                            "request_id": r.request_id,
                        }
                        for r in page.items
                    ]
                    format_output(items, format_type, headers)
                else:
                    click.echo("No results found")
            else:
                output = {
                    "total": page.total,
                    "items": [
                        {
                            "id": r.id,
                            "request_id": r.request_id,
                            "result_type": r.result_type,
                            "is_valid": r.is_valid,
                            "data": r.data,
                            "validation_errors": r.validation_errors,
                        }
                        for r in page.items
                    ],
                    "limit": limit,
                    "offset": offset,
                    "has_more": page.has_more,
                }
                format_output(output, format_type)

    asyncio.run(run())


@results.command("show")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("result_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.pass_context
def results_show(
    ctx: click.Context, db_path: str | None, result_id: int, format_type: str
) -> None:
    """Show detailed result information.

    \b
    Examples:
        pdd --db run.db results show 123
        pdd --db run.db results show 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            result = await debugger.get_result(result_id)

            if result is None:
                click.echo(f"Result {result_id} not found", err=True)
                sys.exit(1)

            if format_type == "summary":
                click.echo(f"ID: {result.id}")
                click.echo(f"Request ID: {result.request_id}")
                click.echo(f"Type: {result.result_type}")
                click.echo(f"Valid: {'Yes' if result.is_valid else 'No'}")
                click.echo(f"Data: {json.dumps(result.data, indent=2)}")
                if result.validation_errors:
                    click.echo(
                        f"Validation Errors: {json.dumps(result.validation_errors, indent=2)}"
                    )
                click.echo(f"Created At: {result.created_at}")
            else:
                output = {
                    "id": result.id,
                    "request_id": result.request_id,
                    "result_type": result.result_type,
                    "is_valid": result.is_valid,
                    "data": result.data,
                    "validation_errors": result.validation_errors,
                    "created_at": result.created_at,
                }
                format_output(output, format_type)

    asyncio.run(run())


@results.command("summary")
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
def results_summary(
    ctx: click.Context, db_path: str | None, format_type: str
) -> None:
    """Show result counts by type and validity.

    \b
    Examples:
        pdd --db run.db results summary
        pdd --db run.db results summary --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            summary = await debugger.get_result_summary()

            if format_type == "summary":
                for result_type, counts in summary.items():
                    click.echo(f"\n{result_type}:")
                    for status, count in counts.items():
                        click.echo(f"  {status}: {count}")
            else:
                format_output(summary, format_type)

    asyncio.run(run())


@results.command("export")
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
def results_export(
    ctx: click.Context,
    db_path: str | None,
    output_path: str,
    result_type: str | None,
    valid: bool | None,
) -> None:
    """Export results to JSONL (newline-delimited JSON) file.

    \b
    Examples:
        pdd --db run.db results export results.jsonl
        pdd --db run.db results export opinions.jsonl --type CourtOpinion --valid
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            count = await debugger.export_results_jsonl(
                output_path, result_type=result_type, is_valid=valid
            )
            click.echo(f"Exported {count} results to {output_path}")

    asyncio.run(run())


@results.command("validate")
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
def results_validate(
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
        pdd --db run.db results validate
        pdd --db run.db results validate --step parse_opinions_page
        pdd --db run.db results validate --detailed
        pdd --db run.db results validate --request 15
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
