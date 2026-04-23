"""CLI commands for inspecting and exporting results."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    register_cli_group,
)
from kent.driver.persistent_driver.cli._options import (
    db_option,
    format_options,
    pagination_options,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Results Commands
# =========================================================================


results = register_cli_group("results", "Inspect and export results.")


@results.command("list")
@click.option("--type", "result_type", help="Filter by result type")
@click.option(
    "--valid/--invalid", default=None, help="Filter by validation status"
)
@db_option
@format_options
@pagination_options
@click.pass_context
def results_list(
    ctx: click.Context,
    db_path: str | None,
    result_type: str | None,
    valid: bool | None,
    limit: int,
    offset: int,
    format_type: str,
    template_name: str | None,
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
            render_output(
                output,
                format_type=format_type,
                template_path="results/list",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@results.command("show")
@click.argument("result_id", type=int)
@db_option
@format_options
@click.pass_context
def results_show(
    ctx: click.Context,
    db_path: str | None,
    result_id: int,
    format_type: str,
    template_name: str | None,
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

            output = {
                "id": result.id,
                "request_id": result.request_id,
                "result_type": result.result_type,
                "is_valid": result.is_valid,
                "data": result.data,
                "validation_errors": result.validation_errors,
                "created_at": result.created_at,
            }
            render_output(
                output,
                format_type=format_type,
                template_path="results/show",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@results.command("summary")
@db_option
@format_options
@click.pass_context
def results_summary(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
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

            render_output(
                summary,
                format_type=format_type,
                template_path="results/summary",
                template_name=template_name or "default",
            )

    asyncio.run(run())


@results.command("export")
@click.argument("output_path", type=click.Path())
@click.option("--type", "result_type", help="Filter by result type")
@click.option(
    "--valid/--invalid", default=None, help="Filter by validation status"
)
@db_option
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
@db_option
@format_options
@click.pass_context
def results_validate(
    ctx: click.Context,
    db_path: str | None,
    format_type: str,
    template_name: str | None,
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

                target = (
                    f"request {request_id}"
                    if request_id is not None
                    else f"response {response_id}"
                )
                output = {
                    "mode": "detail",
                    "target": target,
                    "detail": detail,
                }
                render_output(
                    output,
                    format_type=format_type,
                    template_path="results/validate",
                    template_name=template_name or "default",
                )
                return

            # Summary / detailed mode
            result = await debugger.validate_structure(
                step_name=step_name,
            )

            output = {
                "mode": "summary",
                "detailed": detailed,
                "steps": result["steps"],
                "summary": result["summary"],
            }
            render_output(
                output,
                format_type=format_type,
                template_path="results/validate",
                template_name=template_name or "default",
            )

    asyncio.run(run())
