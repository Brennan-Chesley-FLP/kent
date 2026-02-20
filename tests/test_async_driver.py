"""Tests for AsyncDriver.

This module tests the AsyncDriver implementation, focusing on:
- Basic functionality matching SyncDriver
- Multiple worker concurrency
- Graceful shutdown via stop_event
- Priority queue ordering
"""

import asyncio
from collections.abc import Generator
from pathlib import Path

import pytest

from kent.data_types import (
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.async_driver import AsyncDriver
from tests.utils import collect_results_async


class TestAsyncDriverBasic:
    """Tests for basic AsyncDriver functionality."""

    @pytest.mark.asyncio
    async def test_async_driver_processes_single_request(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall process a single request and return data."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"success": True, "url": response.url})

        scraper = SimpleScraper()
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        await driver.run()

        assert len(results) == 1
        assert results[0]["success"] is True

    @pytest.mark.asyncio
    async def test_async_driver_processes_multiple_pages(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall process multiple pages yielded from entry."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                for i in range(1, 4):
                    yield Request(
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
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        await driver.run()

        assert len(results) == 3


class TestAsyncDriverStopEvent:
    """Tests for stop_event functionality."""

    @pytest.mark.asyncio
    async def test_driver_stops_when_event_set_before_start(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall not process requests when stop_event is set before run()."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"processed": True})

        scraper = SimpleScraper()
        callback, results = collect_results_async()
        stop_event = asyncio.Event()

        # Set stop event before running
        stop_event.set()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            stop_event=stop_event,
        )

        await driver.run()

        # No results should be collected since we stopped immediately
        assert len(results) == 0

    @pytest.mark.asyncio
    async def test_driver_completes_when_stop_event_not_set(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall complete all requests when stop_event is not set."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                for i in range(1, 4):
                    yield Request(
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
        callback, results = collect_results_async()
        stop_event = asyncio.Event()

        # Don't set the stop event - driver should complete normally
        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            stop_event=stop_event,
        )

        await driver.run()

        # All 3 pages should be processed
        assert len(results) == 3


class TestAsyncDriverWorkers:
    """Tests for multiple worker functionality."""

    @pytest.mark.asyncio
    async def test_single_worker_processes_all_requests(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver with one worker shall process all requests."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                for i in range(1, 6):
                    yield Request(
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
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            num_workers=1,
        )

        await driver.run()

        assert len(results) == 5

    @pytest.mark.asyncio
    async def test_multiple_workers_process_all_requests(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver with multiple workers shall process all requests."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                for i in range(1, 11):
                    yield Request(
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
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            num_workers=4,
        )

        await driver.run()

        # All 10 pages should be processed
        assert len(results) == 10
        # Verify all pages were processed (order may vary with multiple workers)
        pages = {r["page"] for r in results}
        assert pages == set(range(1, 11))


class TestAsyncDriverPriority:
    """Tests for priority queue ordering."""

    @pytest.mark.asyncio
    async def test_priority_ordering_with_single_worker(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall process requests in priority order with single worker."""

        class PriorityScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield requests with different priorities
                # Priority 9 (default)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/low",
                    ),
                    continuation="parse_low",
                    priority=9,
                )
                # Priority 1 (high)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/high",
                    ),
                    continuation="parse_high",
                    priority=1,
                )
                # Priority 5 (medium)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/medium",
                    ),
                    continuation="parse_medium",
                    priority=5,
                )

            def parse_high(self, response: Response):
                yield ParsedData(data={"order": 1, "priority": 1})

            def parse_medium(self, response: Response):
                yield ParsedData(data={"order": 2, "priority": 5})

            def parse_low(self, response: Response):
                yield ParsedData(data={"order": 3, "priority": 9})

        scraper = PriorityScraper()
        callback, results = collect_results_async()

        # Use single worker to ensure deterministic ordering
        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            num_workers=1,
        )

        await driver.run()

        # Verify order: priority 1, then 5, then 9
        assert len(results) == 3
        assert results[0]["priority"] == 1
        assert results[1]["priority"] == 5
        assert results[2]["priority"] == 9


class TestAsyncDriverArchive:
    """Tests for archive request handling."""

    @pytest.mark.asyncio
    async def test_archive_request_saves_file(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall handle archive Request and save files."""

        class ArchiveScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(
                    data={
                        "type": "archive",
                        "file_url": response.file_url,
                    }
                )

        scraper = ArchiveScraper()
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        await driver.run()

        assert len(results) == 1
        assert results[0]["type"] == "archive"
        assert results[0]["file_url"] is not None


class TestAsyncDriverLifecycle:
    """Tests for lifecycle callbacks."""

    @pytest.mark.asyncio
    async def test_lifecycle_callbacks_fire(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The async driver shall fire on_run_start and on_run_complete callbacks."""
        lifecycle_events: list[str] = []

        async def on_run_start(scraper_name: str) -> None:
            lifecycle_events.append(f"start:{scraper_name}")

        async def on_run_complete(
            scraper_name: str,
            status: str,
            error: Exception | None,
        ) -> None:
            lifecycle_events.append(f"complete:{scraper_name}:{status}")

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                yield ParsedData(data={"success": True})

        scraper = SimpleScraper()
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_run_start=on_run_start,
            on_run_complete=on_run_complete,
        )

        await driver.run()

        assert len(lifecycle_events) == 2
        assert lifecycle_events[0] == "start:SimpleScraper"
        assert lifecycle_events[1].startswith(
            "complete:SimpleScraper:completed"
        )
