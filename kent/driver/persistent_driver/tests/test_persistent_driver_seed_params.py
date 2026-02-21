"""Tests for seed_params filtering in PersistentDriver.run().

Verifies that seed_params correctly controls which entries and
speculative functions are executed during a run.

Combinations tested:
- seed_params=None → all entries + all speculation runs
- seed_params with only non-speculative entries → only those entries, no speculation
- seed_params with only speculative entry → no initial_seed, speculation only
- seed_params with both speculative and non-speculative → both run
- seed_params=[] (empty list) → nothing runs
"""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import sqlalchemy as sa

from kent.common.decorators import entry, step
from kent.common.speculation_types import SimpleSpeculation
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.driver.persistent_driver.persistent_driver import PersistentDriver
from kent.driver.persistent_driver.testing import (
    TestRequestManager,
    create_html_response,
)

# ---------------------------------------------------------------------------
# Test scraper with mixed entry types
# ---------------------------------------------------------------------------


class MixedEntryScraper(BaseScraper[dict]):
    """Scraper with one speculative entry and two non-speculative entries."""

    @entry(
        dict,
        speculative=SimpleSpeculation(
            highest_observed=3,
            largest_observed_gap=2,
        ),
    )
    def fetch_case(self, case_id: int) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/case/{case_id}",
            ),
            continuation="parse_case",
            is_speculative=True,
        )

    @step
    def parse_case(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        case_id = int(response.url.split("/")[-1])
        yield ParsedData({"type": "case", "id": case_id})

    @entry(dict)
    def get_justices(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/justices",
            ),
            continuation="parse_justices",
            current_location="",
        )

    @step
    def parse_justices(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"type": "justice"})

    @entry(dict)
    def get_oral_arguments(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/oral-arguments",
            ),
            continuation="parse_oral_arguments",
            current_location="",
        )

    @step
    def parse_oral_arguments(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"type": "oral_argument"})


# ---------------------------------------------------------------------------
# Helper to build a request manager that answers all URLs
# ---------------------------------------------------------------------------


def _make_request_manager() -> TestRequestManager:
    """Build a TestRequestManager that responds to all expected URLs."""
    manager = TestRequestManager()
    manager.add_response(
        "https://example.com/justices",
        create_html_response("<html>Justices</html>"),
    )
    manager.add_response(
        "https://example.com/oral-arguments",
        create_html_response("<html>Oral Arguments</html>"),
    )
    # Speculative case URLs
    for i in range(1, 20):
        manager.add_response(
            f"https://example.com/case/{i}",
            create_html_response(f"<html>Case {i}</html>"),
        )
    return manager


async def _store_seed_params(
    driver: PersistentDriver[Any],
    seed_params: list[dict[str, Any]] | None,
) -> None:
    """Store seed_params directly in run_metadata via raw SQL.

    PersistentDriver.open() calls init_run_metadata without seed_params.
    The web UI path stores them separately. For tests we inject them
    directly into the row that open() already created.
    """
    import json

    if seed_params is None:
        return
    async with driver.db._session_factory() as session:
        await session.execute(
            sa.text(
                "UPDATE run_metadata SET seed_params_json = :val WHERE id = 1"
            ),
            {"val": json.dumps(seed_params)},
        )
        await session.commit()


def _requested_urls(manager: TestRequestManager) -> set[str]:
    """Return the set of URLs that were requested."""
    return {r.request.url for r in manager.requests}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestSeedParamsNone:
    """seed_params=None → default behavior: all entries + all speculation."""

    async def test_all_entries_run(self, db_path: Path) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            # No seed_params stored → None
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        # Non-speculative entries should have been called
        assert "https://example.com/justices" in urls
        assert "https://example.com/oral-arguments" in urls
        # Speculation should have seeded case URLs (at least ID 1)
        assert "https://example.com/case/1" in urls


class TestSeedParamsNonSpecOnly:
    """seed_params with only non-speculative entries selected."""

    async def test_only_selected_entries_run(self, db_path: Path) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await _store_seed_params(driver, [{"get_justices": {}}])
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        # Only get_justices should have run
        assert "https://example.com/justices" in urls
        # get_oral_arguments was NOT selected
        assert "https://example.com/oral-arguments" not in urls
        # Speculation was NOT selected
        assert "https://example.com/case/1" not in urls

    async def test_both_non_spec_entries(self, db_path: Path) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await _store_seed_params(
                driver,
                [{"get_justices": {}}, {"get_oral_arguments": {}}],
            )
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        assert "https://example.com/justices" in urls
        assert "https://example.com/oral-arguments" in urls
        # Speculation was NOT selected
        assert "https://example.com/case/1" not in urls


class TestSeedParamsSpeculativeOnly:
    """seed_params with only the speculative entry → no initial_seed crash."""

    async def test_speculation_runs_without_initial_seed(
        self, db_path: Path
    ) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await _store_seed_params(driver, [{"fetch_case": {}}])
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        # Non-speculative entries should NOT have run
        assert "https://example.com/justices" not in urls
        assert "https://example.com/oral-arguments" not in urls
        # Speculation should have seeded case URLs
        assert "https://example.com/case/1" in urls


class TestSeedParamsBothTypes:
    """seed_params with both speculative and non-speculative entries."""

    async def test_both_types_run(self, db_path: Path) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await _store_seed_params(
                driver,
                [{"fetch_case": {}}, {"get_justices": {}}],
            )
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        # get_justices should have run
        assert "https://example.com/justices" in urls
        # get_oral_arguments was NOT selected
        assert "https://example.com/oral-arguments" not in urls
        # Speculation for fetch_case should have run
        assert "https://example.com/case/1" in urls


class TestSeedParamsEmpty:
    """seed_params=[] (empty list) → no entries run, no speculation."""

    async def test_empty_list_runs_nothing(self, db_path: Path) -> None:
        manager = _make_request_manager()
        scraper = MixedEntryScraper()

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await _store_seed_params(driver, [])
            await driver.run(setup_signal_handlers=False)

        urls = _requested_urls(manager)
        # Nothing should have run
        assert "https://example.com/justices" not in urls
        assert "https://example.com/oral-arguments" not in urls
        assert "https://example.com/case/1" not in urls
