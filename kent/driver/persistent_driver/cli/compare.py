"""CLI commands for comparing and diagnosing scraper runs."""

from __future__ import annotations

import asyncio
import sys

import click

from kent.driver.persistent_driver.cli import (
    _format_data_diff,
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.cli.templating import render_output
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Compare Command
# =========================================================================


@cli.command()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("continuation")
@click.option(
    "--request-id", type=int, help="Compare specific request ID only"
)
@click.option(
    "--sample",
    type=int,
    help="Sample N requests and follow their entire request trees",
)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["default", "summary", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.option(
    "--show-requests",
    is_flag=True,
    help="Show only request tree differences",
)
@click.option("--show-data", is_flag=True, help="Show only data differences")
@click.option(
    "--limit", type=int, help="Maximum number of requests to compare"
)
@click.option(
    "--scraper-class",
    help="Scraper class path (e.g., juriscraper.opinions.united_states.federal_appellate.ca1.Site)",
)
@click.pass_context
def compare(
    ctx: click.Context,
    db_path: str | None,
    continuation: str,
    request_id: int | None,
    sample: int | None,
    format_type: str,
    template_name: str | None,
    show_requests: bool,
    show_data: bool,
    limit: int | None,
    scraper_class: str | None,
) -> None:
    """Compare continuation output between stored and dry-run execution.

    Replays stored responses through current continuation code and compares
    the output (child requests, ParsedData, errors) against stored results.

    \b
    Examples:
        # Compare all requests for a continuation
        ldd-debug compare run.db parse_opinions

        # Compare a specific request
        ldd-debug compare run.db parse_opinions --request-id 123

        # Sample 10 terminal requests
        ldd-debug compare run.db parse_opinions --sample 10

        # Show detailed output
        ldd-debug compare run.db parse_opinions --template detail

        # Show only request changes
        ldd-debug compare run.db parse_opinions --show-requests

        # Limit to 50 comparisons
        ldd-debug compare run.db parse_opinions --limit 50
    """

    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        import importlib

        from kent.driver.persistent_driver.comparison import (
            ComparisonResult,
            ComparisonSummary,
        )

        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Load scraper class
            if scraper_class:
                # Parse module.Class format
                try:
                    module_path, class_name = scraper_class.rsplit(".", 1)
                    module = importlib.import_module(module_path)
                    scraper_cls = getattr(module, class_name)
                except (ValueError, ImportError, AttributeError) as e:
                    click.echo(
                        f"Error: Cannot import scraper class '{scraper_class}': {e}",
                        err=True,
                    )
                    sys.exit(1)
            else:
                # Discover from run metadata
                metadata = await debugger.get_run_metadata()
                if not metadata or not metadata.get("scraper_name"):
                    click.echo(
                        "Error: No scraper_name in run metadata. "
                        "Please provide --scraper-class",
                        err=True,
                    )
                    sys.exit(1)

                scraper_name = metadata["scraper_name"]
                try:
                    # Handle both new format (module:class) and old format (module only)
                    if ":" in scraper_name:
                        module_path, class_name = scraper_name.rsplit(":", 1)
                        module = importlib.import_module(module_path)
                        scraper_cls = getattr(module, class_name)
                    else:
                        # Old format - assume Site class
                        module = importlib.import_module(scraper_name)
                        scraper_cls = module.Site
                except (ImportError, AttributeError) as e:
                    click.echo(
                        f"Error: Cannot import scraper '{scraper_name}': {e}",
                        err=True,
                    )
                    sys.exit(1)

            # Determine which requests to compare
            if request_id is not None:
                # Single request
                request_ids = [request_id]
            elif sample is not None:
                # Sample requests (all completed, since we follow the tree)
                try:
                    request_ids = await debugger.sample_requests(
                        continuation, sample
                    )
                    if not request_ids:
                        click.echo(
                            f"No completed requests found for continuation '{continuation}'",
                            err=True,
                        )
                        sys.exit(1)
                except Exception as e:
                    click.echo(f"Error sampling requests: {e}", err=True)
                    sys.exit(1)
            else:
                # All completed requests for continuation
                page = await debugger.list_requests(
                    status="completed",
                    continuation=continuation,
                    limit=limit or 10000,
                    offset=0,
                )
                request_ids = [r.id for r in page.items]

                if not request_ids:
                    click.echo(
                        f"No completed requests found for continuation '{continuation}'",
                        err=True,
                    )
                    sys.exit(1)

            # Apply limit if specified
            if limit is not None and len(request_ids) > limit:
                request_ids = request_ids[:limit]

            # Perform comparisons - follow entire request tree
            results: list[ComparisonResult] = []
            summary = ComparisonSummary()

            for req_id in request_ids:
                try:
                    # Compare entire tree starting from this request
                    tree_results = await debugger.compare_request_tree(
                        req_id, scraper_cls
                    )
                    for result in tree_results:
                        results.append(result)
                        summary.add_comparison(result)
                except Exception as e:
                    click.echo(
                        f"Warning: Failed to compare request {req_id}: {e}",
                        err=True,
                    )
                    continue

            # Build unified data dict
            def _result_to_dict(r: ComparisonResult) -> dict:
                item: dict = {
                    "request_id": r.request_id,
                    "request_url": r.request_url,
                    "continuation": r.continuation,
                    "has_changes": r.has_changes,
                    "request_diff": {
                        "added": len(r.request_diff.added),
                        "removed": len(r.request_diff.removed),
                        "modified": len(r.request_diff.modified),
                        "unchanged": r.request_diff.unchanged_count,
                        "has_changes": r.request_diff.has_changes,
                        "added_count": len(r.request_diff.added),
                        "removed_count": len(r.request_diff.removed),
                        "modified_count": len(r.request_diff.modified),
                        "added_urls": [
                            req.url for req in r.request_diff.added[:5]
                        ],
                        "removed_urls": [
                            req.url for req in r.request_diff.removed[:5]
                        ],
                        "modified_urls": [
                            orig.url
                            for orig, _new in r.request_diff.modified[:5]
                        ],
                    },
                    "data_diff": {
                        "identical_pairs": r.data_diff.identical_pairs,
                        "changed_pairs": len(r.data_diff.changed_pairs),
                        "added": len(r.data_diff.added),
                        "removed": len(r.data_diff.removed),
                        "has_changes": r.data_diff.has_changes,
                        "added_count": len(r.data_diff.added),
                        "removed_count": len(r.data_diff.removed),
                        "changed_count": len(r.data_diff.changed_pairs),
                        "changed_texts": [
                            _format_data_diff(orig_data.data, new_data.data)
                            for orig_data, new_data, _diffs in r.data_diff.changed_pairs[
                                :3
                            ]
                        ],
                    },
                    "error_diff": {
                        "status": r.error_diff.status,
                        "has_change": r.error_diff.has_change,
                        "original_error_type": (
                            r.error_diff.original_error.error_type
                            if r.error_diff.original_error
                            else None
                        ),
                        "new_error_type": (
                            r.error_diff.new_error.error_type
                            if r.error_diff.new_error
                            else None
                        ),
                    },
                }
                return item

            output = {
                "summary": {
                    "total_requests": summary.total_requests,
                    "identical_outputs": summary.identical_outputs,
                    "requests_with_request_changes": summary.requests_with_request_changes,
                    "requests_with_data_changes": summary.requests_with_data_changes,
                    "errors_introduced": summary.errors_introduced,
                    "errors_resolved": summary.errors_resolved,
                    "errors_changed": summary.errors_changed,
                    "total_request_adds": summary.total_request_adds,
                    "total_request_removes": summary.total_request_removes,
                    "total_request_modifications": summary.total_request_modifications,
                    "total_data_adds": summary.total_data_adds,
                    "total_data_removes": summary.total_data_removes,
                    "total_data_changes": summary.total_data_changes,
                },
                "items": [_result_to_dict(r) for r in results],
                "show_data": show_data,
                "show_requests": show_requests,
            }

            render_output(
                output,
                format_type=format_type,
                template_path="compare/compare",
                template_name=template_name or "default",
            )

    asyncio.run(run())


# =========================================================================
# Diagnose Command
# =========================================================================


@cli.command()
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("error_id", type=int)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["default", "table", "json", "jsonl"]),
    default="default",
    help="Output format",
)
@click.option(
    "--template", "template_name", default=None, help="Template name"
)
@click.pass_context
def diagnose(
    ctx: click.Context,
    db_path: str | None,
    error_id: int,
    format_type: str,
    template_name: str | None,
) -> None:
    """Diagnose an error by re-running XPath observation.

    \b
    Examples:
        ldd-debug diagnose run.db 123
        ldd-debug diagnose run.db 123 --format json
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            try:
                result = await debugger.diagnose(error_id)

                render_output(
                    result,
                    format_type=format_type,
                    template_path="compare/diagnose",
                    template_name=template_name or "default",
                )
            except (ValueError, ImportError) as e:
                click.echo(str(e), err=True)
                sys.exit(1)

    asyncio.run(run())
