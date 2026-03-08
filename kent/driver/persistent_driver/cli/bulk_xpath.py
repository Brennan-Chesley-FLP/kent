"""CLI command for bulk XPath observation statistics."""

from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass, field
from typing import Any

import click

from kent.driver.persistent_driver.cli import (
    _resolve_db_path,
    cli,
)
from kent.driver.persistent_driver.debugger import LocalDevDriverDebugger

# =========================================================================
# Statistics Aggregation
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
    all_observations: list[list[dict[str, Any]]],
) -> tuple[dict[tuple[str, str], SelectorStats], int]:
    """Aggregate query observations across multiple requests.

    Args:
        all_observations: List of per-request query lists (observer.json() output).

    Returns:
        Tuple of (stats dict keyed by (selector, description), count of requests
        with any selector failure).
    """
    stats: dict[tuple[str, str], SelectorStats] = {}
    requests_with_failures = 0

    for queries in all_observations:
        _collect_queries_flat(queries, stats, has_failure_ref=[False])
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
    has_failure_ref: list[bool],
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
            has_failure_ref[0] = True

        if q.get("children"):
            _collect_queries_flat(q["children"], stats, has_failure_ref)


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
# CLI Command
# =========================================================================


@cli.command("bulk-xpath")
@click.option(
    "--db",
    "db_path",
    type=click.Path(exists=True),
    default=None,
    help="Path to the database file",
)
@click.argument("continuation")
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
    "--output-mode",
    type=click.Choice(["summary", "json"]),
    default="summary",
    help="Output mode",
)
@click.pass_context
def bulk_xpath(
    ctx: click.Context,
    db_path: str | None,
    continuation: str,
    request_id: int | None,
    sample: int | None,
    limit: int | None,
    scraper_class: str | None,
    output_mode: str,
) -> None:
    """Gather XPath observation statistics across requests for a continuation.

    Replays stored responses through current continuation code with XPath
    observation active, then aggregates selector statistics across all
    processed requests.

    \b
    Examples:
        # Stats for all requests of a continuation
        ldd-debug bulk-xpath parse_opinions

        # Sample 20 requests
        ldd-debug bulk-xpath parse_opinions --sample 20

        # Specific request
        ldd-debug bulk-xpath parse_opinions --request-id 123

        # JSON output
        ldd-debug bulk-xpath parse_opinions --output-mode json
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

            # Apply limit
            if limit is not None and len(request_ids) > limit:
                request_ids = request_ids[:limit]

            # Run each request with XPath observer
            all_observations: list[list[dict[str, Any]]] = []
            error_count = 0
            total = len(request_ids)

            for i, req_id in enumerate(request_ids, 1):
                try:
                    result = await debugger.run_with_xpath_observer(
                        req_id, scraper_cls
                    )
                    all_observations.append(result["queries"])
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

            # Output
            if output_mode == "json":
                output = {
                    "continuation": continuation,
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
                        }
                        for s in selector_stats.values()
                    ],
                }
                click.echo(json.dumps(output, indent=2))
            else:
                # Summary output
                click.echo(f"\n{'=' * 60}")
                click.echo(f"Bulk XPath Statistics: {continuation}")
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
                else:
                    click.echo("\nNo selector observations recorded.")

    asyncio.run(run())
