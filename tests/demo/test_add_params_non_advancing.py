"""Integration test: ``add_seed_params`` layers a second non-advancing
speculative range onto an existing run.

Scenario:

1. First run against the demo server with
   ``YearlySpeculativeRange(year=2024, min=1, soft_max=6,
   should_advance=False, gap=0)`` — fetches cases 1..5 and stops (no
   advance window).
2. Second run reopens the same DB and calls
   ``driver.add_seed_params([...])`` with ``min=6, soft_max=11,
   should_advance=False, gap=0`` — enqueues cases 6..10 on top of the
   already-completed queue.
3. Final DB state has all 10 cases (``BCC-2024-001`` through
   ``BCC-2024-010``) with no duplicates and no errors — proves
   ``--add-params`` composes cleanly with the non-advancing branch of
   the unified speculation seeder.
"""

from __future__ import annotations

import json
from pathlib import Path

import sqlalchemy as sa

from kent.demo.scraper import BugCourtDemoScraper
from kent.driver.persistent_driver.persistent_driver import PersistentDriver


async def _dockets_in_results(driver: PersistentDriver) -> set[str]:
    """Return the set of dockets stored in the ``results`` table."""
    async with driver.db._session_factory() as session:
        rows = (
            await session.execute(
                sa.text(
                    "SELECT data_json FROM results "
                    "WHERE result_type = 'CaseData'"
                )
            )
        ).all()
    return {json.loads(r[0])["docket"] for r in rows}


async def _dockets_from_db_path(db_path: Path) -> set[str]:
    """Open the DB after-the-fact and pull all CaseData dockets out."""
    from kent.driver.persistent_driver.sql_manager import SQLManager

    async with (
        SQLManager.open(db_path) as sql,
        sql._session_factory() as session,
    ):
        rows = (
            await session.execute(
                sa.text(
                    "SELECT data_json FROM results "
                    "WHERE result_type = 'CaseData'"
                )
            )
        ).all()
    return {json.loads(r[0])["docket"] for r in rows}


async def test_add_params_non_advancing_extends_seed_range(
    demo_server_url: str, tmp_path: Path
) -> None:
    """[1, 6) via --params + [6, 11) via --add-params yields dockets 1..10."""
    db_path = tmp_path / "add_params_non_advancing.db"

    def _make_scraper() -> BugCourtDemoScraper:
        scraper = BugCourtDemoScraper()
        scraper.court_url = demo_server_url  # type: ignore[misc]
        scraper.rate_limits = []  # type: ignore[misc]
        return scraper

    # --- Run 1: non-advancing seed [1, 6) for year 2024 -------------------
    first_params = [
        {
            "fetch_case": {
                "case_id": {
                    "year": 2024,
                    "min": 1,
                    "soft_max": 6,
                    "should_advance": False,
                    "gap": 0,
                }
            }
        }
    ]
    async with PersistentDriver.open(
        _make_scraper(),
        db_path,
        enable_monitor=False,
        seed_params=first_params,
    ) as driver:
        await driver.run(setup_signal_handlers=False)
        first_dockets = await _dockets_in_results(driver)

    assert first_dockets == {f"BCC-2024-{n:03d}" for n in range(1, 6)}, (
        f"First run produced unexpected dockets: {sorted(first_dockets)}"
    )

    # --- Run 2: add_seed_params with [6, 11) ------------------------------
    add_params = [
        {
            "fetch_case": {
                "case_id": {
                    "year": 2024,
                    "min": 6,
                    "soft_max": 11,
                    "should_advance": False,
                    "gap": 0,
                }
            }
        }
    ]
    async with PersistentDriver.open(
        _make_scraper(),
        db_path,
        enable_monitor=False,
    ) as driver:
        await driver.add_seed_params(add_params)
        await driver.run(setup_signal_handlers=False)

    final_dockets = await _dockets_from_db_path(db_path)

    expected = {f"BCC-2024-{n:03d}" for n in range(1, 11)}
    assert final_dockets == expected, (
        f"Expected dockets {sorted(expected)}, got {sorted(final_dockets)}; "
        f"missing={sorted(expected - final_dockets)}, "
        f"extra={sorted(final_dockets - expected)}"
    )

    # Also verify the errors table is empty — none of these cases should
    # have produced errors (all 10 exist on the demo server).
    from kent.driver.persistent_driver.sql_manager import SQLManager

    async with (
        SQLManager.open(db_path) as sql,
        sql._session_factory() as session,
    ):
        err_count = (
            await session.execute(sa.text("SELECT COUNT(*) FROM errors"))
        ).scalar_one()
    assert err_count == 0, f"Expected no errors, got {err_count}"


async def test_two_non_advancing_templates_in_one_run(
    demo_server_url: str, tmp_path: Path
) -> None:
    """Two disjoint non-advancing seed ranges for the same @entry yield
    the union of their IDs — here cases 2024-001..005 and 2024-007..010,
    nine total, deliberately skipping 2024-006."""
    db_path = tmp_path / "two_non_advancing.db"

    scraper = BugCourtDemoScraper()
    scraper.court_url = demo_server_url  # type: ignore[misc]
    scraper.rate_limits = []  # type: ignore[misc]

    seed_params = [
        {
            "fetch_case": {
                "case_id": {
                    "year": 2024,
                    "min": 1,
                    "soft_max": 6,
                    "should_advance": False,
                    "gap": 0,
                }
            }
        },
        {
            "fetch_case": {
                "case_id": {
                    "year": 2024,
                    "min": 7,
                    "soft_max": 11,
                    "should_advance": False,
                    "gap": 0,
                }
            }
        },
    ]
    async with PersistentDriver.open(
        scraper,
        db_path,
        enable_monitor=False,
        seed_params=seed_params,
    ) as driver:
        await driver.run(setup_signal_handlers=False)

    final_dockets = await _dockets_from_db_path(db_path)

    expected = {f"BCC-2024-{n:03d}" for n in [1, 2, 3, 4, 5, 7, 8, 9, 10]}
    assert final_dockets == expected, (
        f"Expected dockets {sorted(expected)}, got {sorted(final_dockets)}; "
        f"missing={sorted(expected - final_dockets)}, "
        f"extra={sorted(final_dockets - expected)}"
    )
    # BCC-2024-006 must not have been fetched — the gap between the two
    # templates is explicit and the non-advancing flag means no probing.
    assert "BCC-2024-006" not in final_dockets

    from kent.driver.persistent_driver.sql_manager import SQLManager

    async with (
        SQLManager.open(db_path) as sql,
        sql._session_factory() as session,
    ):
        err_count = (
            await session.execute(sa.text("SELECT COUNT(*) FROM errors"))
        ).scalar_one()
    assert err_count == 0, f"Expected no errors, got {err_count}"
