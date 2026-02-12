"""Tests for Step 15: Priority Queue.

This module tests the priority queue implementation that optimizes memory
consumption by processing high-priority requests first.

Key behaviors tested:
- Requests are processed in priority order (lower number = higher priority)
- ArchiveRequests (priority 1) are processed before regular requests (priority 9)
- FIFO ordering is maintained within the same priority level
- Custom priorities can be set on requests
- Priority is preserved through request resolution
"""

from collections.abc import Generator
from pathlib import Path

from kent.data_types import (
    ArchiveRequest,
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
    ParsedData,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestPriorityOrdering:
    """Tests for priority-based request ordering."""

    def test_lower_priority_number_processed_first(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall process requests with lower priority numbers first."""

        class PriorityScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield requests with different priorities
                # Priority 9 (default)
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/low",
                    ),
                    continuation="parse_low",
                    priority=9,
                )
                # Priority 1 (high)
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/high",
                    ),
                    continuation="parse_high",
                    priority=1,
                )
                # Priority 5 (medium)
                yield NavigatingRequest(
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
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Verify order: priority 1, then 5, then 9
        assert len(results) == 3
        assert results[0]["priority"] == 1
        assert results[1]["priority"] == 5
        assert results[2]["priority"] == 9

    def test_archive_requests_have_priority_1(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The ArchiveRequest shall have default priority of 1."""

        class ArchivePriorityScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield regular request (priority 9) and archive request (priority 1)
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/regular",
                    ),
                    continuation="parse_regular",
                )
                yield ArchiveRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"type": "archive", "order": 1})

            def parse_regular(self, response: Response):
                yield ParsedData(data={"type": "regular", "order": 2})

        scraper = ArchivePriorityScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Archive request should be processed before regular request
        assert len(results) == 2
        assert results[0]["type"] == "archive"
        assert results[1]["type"] == "regular"

    def test_fifo_ordering_within_same_priority(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall maintain FIFO ordering for requests with the same priority."""

        class FIFOScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield multiple requests with same priority
                for i in range(1, 6):
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/page{i}",
                        ),
                        continuation="parse_page",
                        priority=5,
                        accumulated_data={"sequence": i},
                    )

            def parse_page(self, response: Response):
                yield ParsedData(
                    data={
                        "sequence": response.request.accumulated_data[
                            "sequence"
                        ]
                    }
                )

        scraper = FIFOScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Verify FIFO order (1, 2, 3, 4, 5)
        assert len(results) == 5
        for i in range(5):
            assert results[i]["sequence"] == i + 1

    def test_priority_preserved_through_resolution(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The priority shall be preserved when requests are resolved."""

        class ResolutionScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield request with custom priority
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/custom",
                    ),
                    continuation="parse_custom",
                    priority=3,
                )
                # Yield default priority request
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/default",
                    ),
                    continuation="parse_default",
                )

            def parse_custom(self, response: Response):
                yield ParsedData(data={"type": "custom", "order": 1})

            def parse_default(self, response: Response):
                yield ParsedData(data={"type": "default", "order": 2})

        scraper = ResolutionScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Custom priority (3) should come before default (9)
        assert len(results) == 2
        assert results[0]["type"] == "custom"
        assert results[1]["type"] == "default"


class TestMemoryOptimization:
    """Tests for memory optimization through priority ordering."""

    def test_high_priority_requests_clear_queue_faster(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall process high-priority terminal requests to reduce queue size."""

        class OptimizationScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Yield archive requests (priority 1) that are terminal
                for i in range(1, 4):
                    yield ArchiveRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/files/file{i}.pdf",
                        ),
                        continuation="parse_archive",
                        expected_type="pdf",
                        accumulated_data={"file_id": i},
                    )
                # Yield navigating request (priority 9) that creates more requests
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/more",
                    ),
                    continuation="parse_more",
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(
                    data={
                        "type": "archive",
                        "file_id": response.request.accumulated_data[
                            "file_id"
                        ],
                    }
                )

            def parse_more(self, response: Response):
                # This creates more work (but is processed last)
                yield ParsedData(data={"type": "more"})

        scraper = OptimizationScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # All archive requests processed before navigating request
        assert len(results) == 4
        assert results[0]["type"] == "archive"
        assert results[1]["type"] == "archive"
        assert results[2]["type"] == "archive"
        assert results[3]["type"] == "more"


class TestDefaultPriorities:
    """Tests for default priority values."""

    def test_base_request_default_priority_is_9(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The BaseRequest shall have a default priority of 9."""

        class DefaultPriorityScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                request = NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )
                # Verify default priority before yielding
                assert request.priority == 9
                yield request

            def parse(self, response: Response):
                yield ParsedData(data={"success": True})

        scraper = DefaultPriorityScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1

    def test_archive_request_default_priority_is_1(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The ArchiveRequest shall have a default priority of 1."""

        class ArchiveDefaultScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                archive_request = ArchiveRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                )
                # Verify default priority
                assert archive_request.priority == 1
                yield archive_request

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"success": True})

        scraper = ArchiveDefaultScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
