"""CLI commands for step-level development tools."""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any

import click

from kent.driver.persistent_driver.cli import (
    _format_data_diff,
    _resolve_db_path,
    cli,
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
def step(ctx: click.Context, db_path: str | None) -> None:
    """Step-level development and debugging tools."""
    ctx.ensure_object(dict)
    if db_path:
        ctx.obj["db_path"] = db_path


# =========================================================================
# Re-evaluate Command (moved from compare.py)
# =========================================================================


@step.command("re-evaluate")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step_name")
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
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    help="Show detailed per-request changes",
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
def re_evaluate(
    ctx: click.Context,
    db_path: str | None,
    step_name: str,
    request_id: int | None,
    sample: int | None,
    format_type: str,
    verbose: bool,
    show_requests: bool,
    show_data: bool,
    limit: int | None,
    scraper_class: str | None,
) -> None:
    """Compare step output between stored and dry-run execution.

    Replays stored responses through current step code and compares
    the output (child requests, ParsedData, errors) against stored results.

    \b
    Examples:
        # Compare all requests for a step
        pdd step re-evaluate --db run.db parse_opinions

        # Compare a specific request
        pdd step re-evaluate --db run.db parse_opinions --request-id 123

        # Sample 10 request trees randomly
        pdd step re-evaluate --db run.db parse_opinions --sample 10

        # Show detailed per-request changes
        pdd step re-evaluate --db run.db parse_opinions --verbose

        # Show only request tree differences
        pdd step re-evaluate --db run.db parse_opinions --show-requests

        # Show only data differences
        pdd step re-evaluate --db run.db parse_opinions --show-data

        # Limit to 50 comparisons
        pdd step re-evaluate --db run.db parse_opinions --limit 50
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
                        step_name, sample
                    )
                    if not request_ids:
                        click.echo(
                            f"No completed requests found for step '{step_name}'",
                            err=True,
                        )
                        sys.exit(1)
                except Exception as e:
                    click.echo(f"Error sampling requests: {e}", err=True)
                    sys.exit(1)
            else:
                # All completed requests for step
                page = await debugger.list_requests(
                    status="completed",
                    continuation=step_name,
                    limit=limit or 10000,
                    offset=0,
                )
                request_ids = [r.id for r in page.items]

                if not request_ids:
                    click.echo(
                        f"No completed requests found for step '{step_name}'",
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

            # Output results
            if format_type in ("json", "jsonl"):
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
                    "results": [
                        {
                            "request_id": r.request_id,
                            "request_url": r.request_url,
                            "continuation": r.continuation,
                            "has_changes": r.has_changes,
                            "request_diff": {
                                "added": len(r.request_diff.added),
                                "removed": len(r.request_diff.removed),
                                "modified": len(r.request_diff.modified),
                                "unchanged": r.request_diff.unchanged_count,
                            },
                            "data_diff": {
                                "identical_pairs": r.data_diff.identical_pairs,
                                "changed_pairs": len(
                                    r.data_diff.changed_pairs
                                ),
                                "added": len(r.data_diff.added),
                                "removed": len(r.data_diff.removed),
                            },
                            "error_diff": {
                                "status": r.error_diff.status,
                            },
                        }
                        for r in results
                    ],
                }
                if format_type == "json":
                    click.echo(json.dumps(output, indent=2))
                else:
                    for item in output["results"]:
                        click.echo(json.dumps(item))

            else:
                # Summary output (default)
                click.echo(f"\n{'=' * 60}")
                click.echo("Comparison Summary")
                click.echo(f"{'=' * 60}")
                click.echo(f"Total Requests: {summary.total_requests}")
                click.echo(f"Identical Outputs: {summary.identical_outputs}")
                click.echo(
                    f"Requests with Changes: {summary.total_requests - summary.identical_outputs}"
                )

                if not show_data:
                    click.echo("\nRequest Tree Changes:")
                    click.echo(
                        f"  Requests with changes: {summary.requests_with_request_changes}"
                    )
                    click.echo(f"  Total added: {summary.total_request_adds}")
                    click.echo(
                        f"  Total removed: {summary.total_request_removes}"
                    )
                    click.echo(
                        f"  Total modified: {summary.total_request_modifications}"
                    )

                if not show_requests:
                    click.echo("\nData Changes:")
                    click.echo(
                        f"  Requests with changes: {summary.requests_with_data_changes}"
                    )
                    click.echo(f"  Total added: {summary.total_data_adds}")
                    click.echo(
                        f"  Total removed: {summary.total_data_removes}"
                    )
                    click.echo(
                        f"  Total changed: {summary.total_data_changes}"
                    )

                click.echo("\nError Changes:")
                click.echo(f"  Errors introduced: {summary.errors_introduced}")
                click.echo(f"  Errors resolved: {summary.errors_resolved}")
                click.echo(f"  Errors changed: {summary.errors_changed}")

                # Verbose: show per-request detail
                if verbose:
                    for result in results:
                        if not result.has_changes:
                            continue

                        click.echo(f"\n{'=' * 60}")
                        click.echo(f"Request ID: {result.request_id}")
                        click.echo(f"URL: {result.request_url}")
                        click.echo(f"Step: {result.continuation}")

                        # Request changes
                        if result.request_diff.has_changes and not show_data:
                            click.echo("\n  Request Changes:")
                            if result.request_diff.added:
                                click.echo(
                                    f"    Added: {len(result.request_diff.added)} requests"
                                )
                                for req in result.request_diff.added[:5]:
                                    click.echo(f"      + {req.url}")
                            if result.request_diff.removed:
                                click.echo(
                                    f"    Removed: {len(result.request_diff.removed)} requests"
                                )
                                for req in result.request_diff.removed[:5]:
                                    click.echo(f"      - {req.url}")
                            if result.request_diff.modified:
                                click.echo(
                                    f"    Modified: {len(result.request_diff.modified)} requests"
                                )
                                for orig, _new in result.request_diff.modified[
                                    :5
                                ]:
                                    click.echo(f"      ~ {orig.url}")

                        # Data changes
                        if result.data_diff.has_changes and not show_requests:
                            click.echo("\n  Data Changes:")
                            if result.data_diff.added:
                                click.echo(
                                    f"    Added: {len(result.data_diff.added)} results"
                                )
                            if result.data_diff.removed:
                                click.echo(
                                    f"    Removed: {len(result.data_diff.removed)} results"
                                )
                            if result.data_diff.changed_pairs:
                                click.echo(
                                    f"    Changed: {len(result.data_diff.changed_pairs)} results"
                                )
                                for (
                                    orig_data,
                                    new_data,
                                    _diffs,
                                ) in result.data_diff.changed_pairs[:3]:
                                    changes_text = _format_data_diff(
                                        orig_data.data, new_data.data
                                    )
                                    if changes_text:
                                        click.echo(changes_text)

                        # Error changes
                        if result.error_diff.has_change:
                            click.echo("\n  Error Changes:")
                            click.echo(
                                f"    Status: {result.error_diff.status}"
                            )
                            if result.error_diff.original_error:
                                click.echo(
                                    f"    Original: {result.error_diff.original_error.error_type}"
                                )
                            if result.error_diff.new_error:
                                click.echo(
                                    f"    New: {result.error_diff.new_error.error_type}"
                                )

    asyncio.run(run())


# =========================================================================
# XPath Stats Helpers (moved from bulk_xpath.py)
# =========================================================================


@dataclass
class SelectorStats:
    """Aggregated statistics for a single selector across many requests."""

    selector: str
    selector_type: str
    description: str
    expected_min: int
    expected_max: int | None
    times_seen: int = 0
    match_counts: list[int] = field(default_factory=list)
    failures: int = 0
    zero_match_request_ids: list[int] = field(default_factory=list)

    @property
    def total_matches(self) -> int:
        return sum(self.match_counts)

    @property
    def min_matches(self) -> int:
        return min(self.match_counts) if self.match_counts else 0

    @property
    def max_matches(self) -> int:
        return max(self.match_counts) if self.match_counts else 0

    @property
    def avg_matches(self) -> float:
        return (
            self.total_matches / len(self.match_counts)
            if self.match_counts
            else 0.0
        )

    @property
    def failure_rate(self) -> float:
        return self.failures / self.times_seen if self.times_seen else 0.0


def _is_failure(query: dict[str, Any]) -> bool:
    """Check if a query's match count violates its expectations."""
    count = query["match_count"]
    if count < query["expected_min"]:
        return True
    return query["expected_max"] is not None and count > query["expected_max"]


def _aggregate_queries(
    all_observations: list[tuple[int, list[dict[str, Any]]]],
) -> tuple[dict[tuple[str, str], SelectorStats], int]:
    """Aggregate query observations across multiple requests.

    Args:
        all_observations: List of (request_id, query_list) tuples.

    Returns:
        Tuple of (stats dict keyed by (selector, description), count of requests
        with any selector failure).
    """
    stats: dict[tuple[str, str], SelectorStats] = {}
    requests_with_failures = 0

    for req_id, queries in all_observations:
        _collect_queries_flat(queries, stats, req_id)
        if _request_has_failure(queries):
            requests_with_failures += 1

    return stats, requests_with_failures


def _request_has_failure(queries: list[dict[str, Any]]) -> bool:
    """Check if any query in a tree has a failure."""
    for q in queries:
        if _is_failure(q):
            return True
        if q.get("children") and _request_has_failure(q["children"]):
            return True
    return False


def _collect_queries_flat(
    queries: list[dict[str, Any]],
    stats: dict[tuple[str, str], SelectorStats],
    request_id: int,
) -> None:
    """Walk query tree and accumulate into stats dict."""
    for q in queries:
        key = (q["selector"], q["description"])
        if key not in stats:
            stats[key] = SelectorStats(
                selector=q["selector"],
                selector_type=q["selector_type"],
                description=q["description"],
                expected_min=q["expected_min"],
                expected_max=q["expected_max"],
            )
        s = stats[key]
        s.times_seen += 1
        s.match_counts.append(q["match_count"])
        if _is_failure(q):
            s.failures += 1
        if q["match_count"] == 0:
            s.zero_match_request_ids.append(request_id)

        if q.get("children"):
            _collect_queries_flat(q["children"], stats, request_id)


def _load_scraper_class(
    scraper_class_path: str | None, metadata: dict[str, Any] | None
) -> type:
    """Load a scraper class from an explicit path or run metadata.

    Args:
        scraper_class_path: Explicit class path (e.g., module.path.Site).
        metadata: Run metadata dict (may contain scraper_name).

    Returns:
        The scraper class.
    """
    import importlib

    if scraper_class_path:
        try:
            module_path, class_name = scraper_class_path.rsplit(".", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        except (ValueError, ImportError, AttributeError) as e:
            click.echo(
                f"Error: Cannot import scraper class '{scraper_class_path}': {e}",
                err=True,
            )
            sys.exit(1)

    if not metadata or not metadata.get("scraper_name"):
        click.echo(
            "Error: No scraper_name in run metadata. "
            "Please provide --scraper-class",
            err=True,
        )
        sys.exit(1)

    scraper_name = metadata["scraper_name"]
    try:
        if ":" in scraper_name:
            module_path, class_name = scraper_name.rsplit(":", 1)
            module = importlib.import_module(module_path)
            return getattr(module, class_name)
        else:
            module = importlib.import_module(scraper_name)
            return module.Site
    except (ImportError, AttributeError) as e:
        click.echo(
            f"Error: Cannot import scraper '{scraper_name}': {e}",
            err=True,
        )
        sys.exit(1)


# =========================================================================
# XPath Stats Command (moved from bulk_xpath.py)
# =========================================================================


@step.command("xpath-stats")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("step_name")
@click.option(
    "--request-id", type=int, help="Run for a specific request ID only"
)
@click.option(
    "--sample",
    type=int,
    help="Sample N requests randomly",
)
@click.option(
    "--limit", type=int, help="Maximum number of requests to process"
)
@click.option(
    "--scraper-class",
    help="Scraper class path (e.g., juriscraper.opinions.united_states.federal_appellate.ca1.Site)",
)
@click.option(
    "--format",
    "format_type",
    type=click.Choice(["summary", "json", "jsonl"]),
    default="summary",
    help="Output format",
)
@click.option(
    "--xpath-name",
    help="Filter output to selectors whose description matches this name",
)
@click.option(
    "--list-non-matching",
    is_flag=True,
    help="Include request IDs where the selector matched zero elements",
)
@click.pass_context
def xpath_stats(
    ctx: click.Context,
    db_path: str | None,
    step_name: str,
    request_id: int | None,
    sample: int | None,
    limit: int | None,
    scraper_class: str | None,
    format_type: str,
    xpath_name: str | None,
    list_non_matching: bool,
) -> None:
    """Gather XPath/selector statistics across requests for a step.

    Replays stored responses through current step code with XPath
    observation active, then aggregates selector match statistics
    across all processed requests.

    \b
    Examples:
        # Stats for all requests of a step
        pdd step xpath-stats --db run.db parse_opinions

        # Sample 20 requests randomly
        pdd step xpath-stats --db run.db parse_opinions --sample 20

        # Filter to a specific selector by description
        pdd step xpath-stats --db run.db parse_opinions --xpath-name "case link"

        # Show which requests had zero matches for a selector
        pdd step xpath-stats --db run.db parse_opinions --xpath-name "case link" --list-non-matching

        # JSON output
        pdd step xpath-stats --db run.db parse_opinions --format json

        # Limit to 100 requests
        pdd step xpath-stats --db run.db parse_opinions --limit 100
    """
    db_path = _resolve_db_path(ctx, db_path)

    async def run() -> None:
        async with LocalDevDriverDebugger.open(db_path) as debugger:
            # Load scraper class
            metadata = (
                await debugger.get_run_metadata()
                if not scraper_class
                else None
            )
            scraper_cls = _load_scraper_class(scraper_class, metadata)

            # Determine which requests to process
            if request_id is not None:
                request_ids = [request_id]
            elif sample is not None:
                try:
                    request_ids = await debugger.sample_requests(
                        step_name, sample
                    )
                    if not request_ids:
                        click.echo(
                            f"No completed requests found for step '{step_name}'",
                            err=True,
                        )
                        sys.exit(1)
                except Exception as e:
                    click.echo(f"Error sampling requests: {e}", err=True)
                    sys.exit(1)
            else:
                page = await debugger.list_requests(
                    status="completed",
                    continuation=step_name,
                    limit=limit or 10000,
                    offset=0,
                )
                request_ids = [r.id for r in page.items]

                if not request_ids:
                    click.echo(
                        f"No completed requests found for step '{step_name}'",
                        err=True,
                    )
                    sys.exit(1)

            # Apply limit
            if limit is not None and len(request_ids) > limit:
                request_ids = request_ids[:limit]

            # Run each request with XPath observer
            all_observations: list[tuple[int, list[dict[str, Any]]]] = []
            error_count = 0
            total = len(request_ids)

            for i, req_id in enumerate(request_ids, 1):
                try:
                    result = await debugger.run_with_xpath_observer(
                        req_id, scraper_cls
                    )
                    all_observations.append((req_id, result["queries"]))
                    if result["error"]:
                        error_count += 1
                except (ValueError, Exception) as e:
                    click.echo(
                        f"Warning: Failed to process request {req_id}: {e}",
                        err=True,
                    )
                    error_count += 1
                    continue

                # Progress indicator for large runs
                if total > 10 and i % max(1, total // 10) == 0:
                    click.echo(
                        f"  Progress: {i}/{total} requests processed...",
                        err=True,
                    )

            # Aggregate statistics
            selector_stats, requests_with_failures = _aggregate_queries(
                all_observations
            )

            # Filter by --xpath-name if provided
            if xpath_name:
                selector_stats = {
                    k: v
                    for k, v in selector_stats.items()
                    if xpath_name.lower() in v.description.lower()
                }

            # Output
            if format_type == "json":
                output: dict[str, Any] = {
                    "continuation": step_name,
                    "requests_processed": len(all_observations),
                    "requests_with_errors": error_count,
                    "requests_with_selector_failures": requests_with_failures,
                    "selectors": [
                        {
                            "selector": s.selector,
                            "selector_type": s.selector_type,
                            "description": s.description,
                            "expected_min": s.expected_min,
                            "expected_max": s.expected_max,
                            "times_seen": s.times_seen,
                            "total_matches": s.total_matches,
                            "min_matches": s.min_matches,
                            "max_matches": s.max_matches,
                            "avg_matches": round(s.avg_matches, 2),
                            "failures": s.failures,
                            "failure_rate": round(s.failure_rate, 4),
                            **(
                                {
                                    "zero_match_request_ids": s.zero_match_request_ids
                                }
                                if list_non_matching
                                else {}
                            ),
                        }
                        for s in selector_stats.values()
                    ],
                }
                click.echo(json.dumps(output, indent=2))
            else:
                # Summary output
                click.echo(f"\n{'=' * 60}")
                click.echo(f"Bulk XPath Statistics: {step_name}")
                click.echo(f"{'=' * 60}")
                click.echo(f"Requests processed: {len(all_observations)}")
                click.echo(f"Requests with errors: {error_count}")
                click.echo(
                    f"Requests with selector failures: {requests_with_failures}"
                )

                if selector_stats:
                    click.echo("\nSelector Statistics:")
                    for s in selector_stats.values():
                        click.echo(f'\n  {s.selector} "{s.description}"')
                        click.echo(
                            f"    Seen: {s.times_seen}  "
                            f"Matches: min={s.min_matches} max={s.max_matches} "
                            f"avg={s.avg_matches:.1f}  "
                            f"Failures: {s.failures}/{s.times_seen} "
                            f"({s.failure_rate:.1%})"
                        )
                        if list_non_matching and s.zero_match_request_ids:
                            click.echo(
                                f"    Zero-match requests: {s.zero_match_request_ids}"
                            )
                else:
                    click.echo("\nNo selector observations recorded.")

    asyncio.run(run())
