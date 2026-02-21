"""Tests for the Bug Civil Court demo.

Verifies that:
1. The demo website serves all expected pages and endpoints.
2. The demo scraper extracts case data, justices, opinions, and oral
   arguments correctly when run against the demo website.
3. Extracted data matches the expected fixture file.
"""

from __future__ import annotations

import json
import socket
import threading
from collections.abc import Generator
from contextlib import closing
from pathlib import Path

import pytest
import uvicorn

from kent.demo.app import app as demo_app
from kent.demo.data import CASES, JUSTICES
from kent.demo.scraper import BugCourtDemoScraper
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results

# ── Server fixture ──────────────────────────────────────────────────


def _find_free_port() -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind(("", 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


@pytest.fixture(scope="module")
def demo_server_url() -> Generator[str, None, None]:
    """Start the demo FastAPI app in a background thread."""
    port = _find_free_port()
    host = "127.0.0.1"

    config = uvicorn.Config(
        demo_app,
        host=host,
        port=port,
        log_level="error",
    )
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()

    # Wait for the server to be ready
    import time

    for _ in range(50):
        try:
            with socket.create_connection((host, port), timeout=0.1):
                break
        except OSError:
            time.sleep(0.1)

    url = f"http://{host}:{port}"
    yield url

    server.should_exit = True
    thread.join(timeout=2.0)


# ── Website smoke tests ─────────────────────────────────────────────


class TestDemoWebsite:
    """Verify the demo website serves expected content."""

    def test_homepage(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/")
        assert r.status_code == 200
        assert "Bug Civil Court" in r.text

    def test_cases_list(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/cases")
        assert r.status_code == 200
        assert "cases-table" in r.text
        # All 30 cases should appear
        for case in CASES:
            assert case.docket in r.text

    def test_case_detail(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/cases/2024/1")
        assert r.status_code == 200
        assert "BCC-2024-001" in r.text
        assert "Beetle v. Ant Colony" in r.text
        assert "case-details" in r.text

    def test_case_not_found(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/cases/2024/999")
        assert r.status_code == 200  # soft 404
        assert "Case Not Found" in r.text

    def test_opinions_list(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/opinions")
        assert r.status_code == 200
        assert "opinions-table" in r.text

    def test_opinion_detail(self, demo_server_url: str):
        import httpx

        # BCC-2024-001 has an opinion
        r = httpx.get(f"{demo_server_url}/opinions/BCC-2024-001")
        assert r.status_code == 200
        assert "opinion-image-link" in r.text

    def test_oral_arguments_list(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/oral-arguments")
        assert r.status_code == 200
        assert "oral-arguments-table" in r.text

    def test_oral_argument_detail(self, demo_server_url: str):
        import httpx

        # BCC-2024-001 has an oral argument
        r = httpx.get(f"{demo_server_url}/oral-arguments/BCC-2024-001")
        assert r.status_code == 200
        assert "audio-download-link" in r.text

    def test_justices_html(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/justices")
        assert r.status_code == 200
        for j in JUSTICES:
            assert j.name in r.text

    def test_justices_json_api(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/api/justices")
        assert r.status_code == 200
        data = r.json()
        assert len(data) == len(JUSTICES)
        assert data[0]["name"] == "Hon. Mantis Green"

    def test_justice_detail_json(self, demo_server_url: str):
        import httpx

        r = httpx.get(f"{demo_server_url}/api/justices/mantis-green")
        assert r.status_code == 200
        data = r.json()
        assert data["title"] == "Chief Justice"


# ── Scraper integration tests ───────────────────────────────────────


class TestDemoScraper:
    """Run the demo scraper against the live demo website."""

    def test_scraper_extracts_all_cases(
        self, demo_server_url: str, tmp_path: Path
    ):
        """The scraper finds all 30 cases via speculation."""
        scraper = BugCourtDemoScraper()
        scraper.court_url = demo_server_url  # type: ignore[misc]
        scraper.rate_limits = []  # type: ignore[misc]

        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )
        driver.run()

        # Extract CaseData results (they'll be DeferredValidation wrappers)
        case_results = [
            r
            for r in results
            if hasattr(r, "docket") and hasattr(r, "plaintiff")
        ]
        dockets = {r.docket for r in case_results}

        expected_dockets = {c.docket for c in CASES}
        assert len(case_results) == 30, (
            f"Expected 30 cases, got {len(case_results)}: "
            f"missing {expected_dockets - dockets}"
        )

    def test_scraper_extracts_justices(
        self, demo_server_url: str, tmp_path: Path
    ):
        """The scraper extracts all justices from the JSON API."""
        scraper = BugCourtDemoScraper()
        scraper.court_url = demo_server_url  # type: ignore[misc]
        scraper.rate_limits = []  # type: ignore[misc]

        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )
        driver.run()

        justice_results = [r for r in results if hasattr(r, "insect_species")]
        assert len(justice_results) == len(JUSTICES)

    def test_fixture_matches_cases(self):
        """The expected_output.json fixture matches the data module."""
        fixture_path = (
            Path(__file__).parent.parent
            / "kent"
            / "demo"
            / "fixtures"
            / "expected_output.json"
        )
        with open(fixture_path) as f:
            fixture = json.load(f)

        assert len(fixture["cases"]) == len(CASES)
        assert len(fixture["justices"]) == len(JUSTICES)

        # Verify each case docket matches
        fixture_dockets = {c["docket"] for c in fixture["cases"]}
        data_dockets = {c.docket for c in CASES}
        assert fixture_dockets == data_dockets

    def test_case_data_matches_fixture(
        self, demo_server_url: str, tmp_path: Path
    ):
        """Scraped case data matches the expected fixture."""
        fixture_path = (
            Path(__file__).parent.parent
            / "kent"
            / "demo"
            / "fixtures"
            / "expected_output.json"
        )
        with open(fixture_path) as f:
            fixture = json.load(f)

        scraper = BugCourtDemoScraper()
        scraper.court_url = demo_server_url  # type: ignore[misc]
        scraper.rate_limits = []  # type: ignore[misc]

        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )
        driver.run()

        case_results = {
            r.docket: r
            for r in results
            if hasattr(r, "docket") and hasattr(r, "plaintiff")
        }

        for expected in fixture["cases"]:
            docket = expected["docket"]
            assert docket in case_results, f"Missing case {docket}"
            actual = case_results[docket]
            assert actual.case_name == expected["case_name"], (
                f"Case name mismatch for {docket}"
            )
            assert actual.plaintiff == expected["plaintiff"], (
                f"Plaintiff mismatch for {docket}"
            )
            assert actual.defendant == expected["defendant"], (
                f"Defendant mismatch for {docket}"
            )
