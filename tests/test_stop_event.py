"""Tests for graceful shutdown via stop_event.

This module tests the stop_event functionality that allows the SyncDriver
to be stopped gracefully from another thread.

Key behaviors tested:
- Driver stops when stop_event is set before processing starts
- Driver stops after completing the current request when stop_event is set
- Driver completes normally when stop_event is never set
"""

import threading
from collections.abc import Generator
from pathlib import Path

from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
    ParsedData,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestStopEventBasic:
    """Tests for basic stop_event functionality."""

    def test_driver_stops_when_event_set_before_start(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall not process any requests when stop_event is set before run()."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"processed": True})

        scraper = SimpleScraper()
        callback, results = collect_results()
        stop_event = threading.Event()

        # Set stop event before running
        stop_event.set()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            stop_event=stop_event,
        )

        driver.run()

        # No results should be collected since we stopped immediately
        assert len(results) == 0

    def test_driver_completes_when_stop_event_not_set(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall complete all requests when stop_event is not set."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                for i in range(1, 4):
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/page{i}",
                        ),
                        continuation="parse_page",
                        accumulated_data={"page": i},
                    )

            def parse_page(self, response: Response):
                yield ParsedData(
                    data={"page": response.request.accumulated_data["page"]}
                )

        scraper = MultiPageScraper()
        callback, results = collect_results()
        stop_event = threading.Event()

        # Don't set the stop event - driver should complete normally
        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            stop_event=stop_event,
        )

        driver.run()

        # All 3 pages should be processed
        assert len(results) == 3

    def test_driver_works_without_stop_event(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall work normally when no stop_event is provided."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"success": True})

        scraper = SimpleScraper()
        callback, results = collect_results()

        # No stop_event provided
        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["success"] is True


class TestStopEventMidRun:
    """Tests for stop_event being set during a run."""

    def test_driver_stops_after_current_request(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall complete the current request before stopping."""
        requests_processed = []

        class TrackingScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                requests_processed.append("entry")
                # Yield multiple requests
                for i in range(1, 6):
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/page{i}",
                        ),
                        continuation="parse_page",
                        accumulated_data={"page": i},
                    )

            def parse_page(self, response: Response):
                page = response.request.accumulated_data["page"]
                requests_processed.append(f"page{page}")
                yield ParsedData(data={"page": page})

        scraper = TrackingScraper()
        callback, results = collect_results()
        stop_event = threading.Event()

        # Use on_data to stop after first page result
        original_callback = callback

        def stopping_callback(data):
            original_callback(data)
            # Stop after first page is processed
            if len(results) >= 1:
                stop_event.set()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=stopping_callback,
            stop_event=stop_event,
        )

        driver.run()

        # Should have processed entry and at least one page
        assert "entry" in requests_processed
        # Should have stopped before processing all 5 pages
        assert len(results) < 5
        # Should have at least 1 result (the one that triggered the stop)
        assert len(results) >= 1
