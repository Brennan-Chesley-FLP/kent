"""Tests for ``PersistentDriver.add_seed_params`` and CLI ``--add-params`` / ``--params`` guards."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path

from click.testing import CliRunner

from kent.cli import cli
from kent.common.decorators import entry, step
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
from kent.driver.persistent_driver.sql_manager import SQLManager
from kent.driver.persistent_driver.testing import (
    MockRequestManager,
    create_html_response,
)


class SimpleScraper(BaseScraper[dict]):
    """Scraper with two selectable @entry functions, no speculation."""

    @entry(dict)
    def get_justices(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url="https://example.com/justices"
            ),
            continuation="parse",
        )

    @entry(dict)
    def get_cases(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url="https://example.com/cases"
            ),
            continuation="parse",
        )

    @step
    def parse(self, response: Response) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"url": response.url})


def _make_manager() -> MockRequestManager:
    m = MockRequestManager()
    m.add_response(
        "https://example.com/justices", create_html_response("<html/>")
    )
    m.add_response(
        "https://example.com/cases", create_html_response("<html/>")
    )
    return m


class TestAddSeedParams:
    async def test_layers_new_entries_onto_existing_run(
        self, db_path: Path
    ) -> None:
        """Initial run uses --params; a subsequent add_seed_params call
        queues and processes the extra entries without re-running the
        originals."""
        # --- First run: seed with get_justices ---
        manager1 = _make_manager()
        async with PersistentDriver.open(
            SimpleScraper(),
            db_path,
            enable_monitor=False,
            request_manager=manager1,
            seed_params=[{"get_justices": {}}],
        ) as driver:
            await driver.run(setup_signal_handlers=False)

        urls1 = {r.request.url for r in manager1.requests}
        assert urls1 == {"https://example.com/justices"}

        # --- Second run: layer on get_cases ---
        manager2 = _make_manager()
        async with PersistentDriver.open(
            SimpleScraper(),
            db_path,
            enable_monitor=False,
            request_manager=manager2,
        ) as driver:
            await driver.add_seed_params([{"get_cases": {}}])
            await driver.run(setup_signal_handlers=False)

        urls2 = {r.request.url for r in manager2.requests}
        # Only the new entry's URL — the old one was already completed
        assert urls2 == {"https://example.com/cases"}

    async def test_merges_into_stored_seed_params(self, db_path: Path) -> None:
        """When seed_params was already stored, add_seed_params merges."""
        async with PersistentDriver.open(
            SimpleScraper(),
            db_path,
            enable_monitor=False,
            request_manager=_make_manager(),
            seed_params=[{"get_justices": {}}],
        ) as driver:
            await driver.add_seed_params([{"get_cases": {}}])

        async with SQLManager.open(db_path) as sql:
            stored = await sql.get_seed_params()
        assert stored == [{"get_justices": {}}, {"get_cases": {}}]

    async def test_leaves_stored_none_untouched(self, db_path: Path) -> None:
        """If no seed_params were stored, add_seed_params doesn't introduce
        filtering — stored remains None so @entry defaults still run."""
        async with PersistentDriver.open(
            SimpleScraper(),
            db_path,
            enable_monitor=False,
            request_manager=_make_manager(),
        ) as driver:
            await driver.add_seed_params([{"get_cases": {}}])

        async with SQLManager.open(db_path) as sql:
            stored = await sql.get_seed_params()
        assert stored is None


# ---------------------------------------------------------------------------
# CLI guards
# ---------------------------------------------------------------------------


TEST_SCRAPER_PATH = (
    "tests.persistent_driver.core.test_add_seed_params:SimpleScraper"
)


class TestCliParamsGuards:
    def test_params_on_existing_db_errors(self, tmp_path: Path) -> None:
        """Running --params against a DB that already has a run is rejected."""
        runner = CliRunner()
        db_path = tmp_path / "run.db"

        # Seed the DB with a real run so that RunMetadata exists
        import asyncio

        async def _seed() -> None:
            async with PersistentDriver.open(
                SimpleScraper(),
                db_path,
                enable_monitor=False,
                request_manager=_make_manager(),
                seed_params=[{"get_justices": {}}],
            ) as driver:
                await driver.run(setup_signal_handlers=False)

        asyncio.run(_seed())

        result = runner.invoke(
            cli,
            [
                "run",
                TEST_SCRAPER_PATH,
                "--driver",
                "persistent",
                "--db",
                str(db_path),
                "--params",
                '[{"get_cases": {}}]',
            ],
        )
        assert result.exit_code != 0
        assert "already has a run" in result.output

    def test_params_and_add_params_mutually_exclusive(
        self, tmp_path: Path
    ) -> None:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "run",
                TEST_SCRAPER_PATH,
                "--driver",
                "persistent",
                "--db",
                str(tmp_path / "run.db"),
                "--params",
                '[{"get_justices": {}}]',
                "--add-params",
                '[{"get_cases": {}}]',
            ],
        )
        assert result.exit_code != 0
        assert "mutually exclusive" in result.output

    def test_add_params_rejected_for_sync_driver(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "run",
                TEST_SCRAPER_PATH,
                "--driver",
                "sync",
                "--add-params",
                '[{"get_cases": {}}]',
            ],
        )
        assert result.exit_code != 0
        assert "--add-params is only supported" in result.output

    def test_add_params_empty_list_rejected(self, tmp_path: Path) -> None:
        runner = CliRunner()
        result = runner.invoke(
            cli,
            [
                "run",
                TEST_SCRAPER_PATH,
                "--driver",
                "persistent",
                "--db",
                str(tmp_path / "run.db"),
                "--add-params",
                "[]",
            ],
        )
        assert result.exit_code != 0
        assert "non-empty JSON list" in result.output
