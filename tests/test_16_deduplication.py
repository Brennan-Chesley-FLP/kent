"""Tests for Step 16: Request Deduplication.

This module tests the deduplication_key field and duplicate_check callback
for preventing duplicate requests.

Key behaviors tested:
- deduplication_key is automatically generated from URL and data
- Custom deduplication_key can be provided
- duplicate_check callback can prevent enqueueing duplicate requests
- Deduplication key generation handles dict and list data consistently
- Same URL with different data produces different keys
- Same URL with same data (different order) produces same key
- SkipDeduplicationCheck can bypass deduplication entirely
"""

from collections.abc import Generator
from pathlib import Path

from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    SkipDeduplicationCheck,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestDedupKeyGeneration:
    """Tests for automatic deduplication key generation."""

    def test_same_url_produces_same_key(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall be identical for requests to the same URL."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test",
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test",
            ),
            continuation="parse",
        )

        assert req1.deduplication_key == req2.deduplication_key

    def test_different_url_produces_different_key(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall differ for requests to different URLs."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test1",
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test2",
            ),
            continuation="parse",
        )

        assert req1.deduplication_key != req2.deduplication_key

    def test_same_url_different_params_produces_different_key(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall differ when query params differ."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/search",
                params={"q": "test", "page": "1"},
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/search",
                params={"q": "test", "page": "2"},
            ),
            continuation="parse",
        )

        assert req1.deduplication_key != req2.deduplication_key

    def test_same_params_different_order_produces_same_key(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall be identical when params are in different order."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/search",
                params={"page": "1", "q": "test"},
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/search",
                params={"q": "test", "page": "1"},
            ),
            continuation="parse",
        )

        assert req1.deduplication_key == req2.deduplication_key

    def test_post_data_dict_sorted_consistently(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall be identical for POST data dicts in different order."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/submit",
                data={"name": "Alice", "age": "30"},
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/submit",
                data={"age": "30", "name": "Alice"},
            ),
            continuation="parse",
        )

        assert req1.deduplication_key == req2.deduplication_key

    def test_post_data_list_sorted_consistently(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall sort list data by first element for consistency."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/submit",
                data=[("name", "Alice"), ("age", "30")],
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/submit",
                data=[("age", "30"), ("name", "Alice")],
            ),
            continuation="parse",
        )

        assert req1.deduplication_key == req2.deduplication_key

    def test_json_data_sorted_consistently(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The deduplication_key shall be identical for JSON data in different order."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/api",
                json={"name": "Alice", "age": 30},
            ),
            continuation="parse",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url=f"{server_url}/api",
                json={"age": 30, "name": "Alice"},
            ),
            continuation="parse",
        )

        assert req1.deduplication_key == req2.deduplication_key


class TestCustomDedupKey:
    """Tests for custom deduplication keys."""

    def test_custom_dedup_key_overrides_default(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The scraper shall be able to provide a custom deduplication_key."""

        req1 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test",
            ),
            continuation="parse",
            deduplication_key="custom-key-1",
        )

        req2 = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{server_url}/test",
            ),
            continuation="parse",
            deduplication_key="custom-key-2",
        )

        # Same URL but different custom keys
        assert req1.deduplication_key == "custom-key-1"
        assert req2.deduplication_key == "custom-key-2"

    def test_custom_dedup_key_preserved_through_resolution(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The custom deduplication_key shall be preserved when request is resolved."""

        class CustomKeyScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield request with custom key
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="/page1",
                    ),
                    continuation="parse_page",
                    deduplication_key="page-1-custom",
                )

            def parse_page(self, response: Response):
                yield ParsedData(data={"url": response.url})

        scraper = CustomKeyScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Verify scraper ran successfully
        assert len(results) == 1


class TestDuplicateCheckCallback:
    """Tests for duplicate_check callback."""

    def test_duplicate_check_prevents_enqueueing_duplicates(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The duplicate_check callback shall prevent duplicate requests from being enqueued."""

        class DuplicateScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield the same request 3 times
                for _ in range(3):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/duplicate",
                        ),
                        continuation="parse_duplicate",
                    )

            def parse_duplicate(self, response: Response):
                yield ParsedData(data={"url": response.url})

        seen_keys: set[str] = set()

        def duplicate_check(dedup_key: str) -> bool:
            """Return False if we've seen this key before."""
            if dedup_key in seen_keys:
                return False  # Skip duplicate
            seen_keys.add(dedup_key)
            return True  # Process first occurrence

        scraper = DuplicateScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            duplicate_check=duplicate_check,
        )

        driver.run()

        # Should only process the duplicate URL once
        assert len(results) == 1

    def test_no_duplicate_check_allows_all_requests(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall enqueue all requests when no duplicate_check is provided."""

        class DuplicateScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield the same request 3 times
                for _ in range(3):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/duplicate",
                        ),
                        continuation="parse_duplicate",
                    )

            def parse_duplicate(self, response: Response):
                yield ParsedData(data={"url": response.url})

        scraper = DuplicateScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            # No duplicate_check callback
        )

        driver.run()

        # All 3 duplicate requests should be processed
        assert len(results) == 3

    def test_duplicate_check_with_different_keys(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The duplicate_check callback shall allow requests with different keys."""

        class MultiPageScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield requests to different pages
                for i in range(1, 4):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/page{i}",
                        ),
                        continuation="parse_page",
                    )

            def parse_page(self, response: Response):
                yield ParsedData(data={"url": response.url})

        seen_keys: set[str] = set()

        def duplicate_check(dedup_key: str) -> bool:
            if dedup_key in seen_keys:
                return False
            seen_keys.add(dedup_key)
            return True

        scraper = MultiPageScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            duplicate_check=duplicate_check,
        )

        driver.run()

        # All 3 different pages should be processed
        assert len(results) == 3

    def test_duplicate_check_with_custom_keys(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The duplicate_check callback shall work with custom deduplication keys."""

        class CustomKeyScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield requests with custom keys
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",
                    ),
                    continuation="parse_page",
                    deduplication_key="custom-1",
                )
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",  # Same URL
                    ),
                    continuation="parse_page",
                    deduplication_key="custom-1",  # Same key
                )
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",  # Same URL
                    ),
                    continuation="parse_page",
                    deduplication_key="custom-2",  # Different key
                )

            def parse_page(self, response: Response):
                yield ParsedData(data={"url": response.url})

        seen_keys: set[str] = set()

        def duplicate_check(dedup_key: str) -> bool:
            if dedup_key in seen_keys:
                return False
            seen_keys.add(dedup_key)
            return True

        scraper = CustomKeyScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            duplicate_check=duplicate_check,
        )

        driver.run()

        # Should process custom-1 once and custom-2 once = 2 total
        assert len(results) == 2


class TestSkipDeduplicationCheck:
    """Tests for SkipDeduplicationCheck behavior."""

    def test_skip_dedup_bypasses_duplicate_check(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """SkipDeduplicationCheck shall bypass the duplicate_check callback entirely."""

        class SkipDedupScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # Yield the same URL 3 times with SkipDeduplicationCheck
                for _ in range(3):
                    yield Request(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"{server_url}/duplicate",
                        ),
                        continuation="parse_duplicate",
                        deduplication_key=SkipDeduplicationCheck(),
                    )

            def parse_duplicate(self, response: Response):
                yield ParsedData(data={"url": response.url})

        seen_keys: set[str] = set()

        def duplicate_check(dedup_key: str) -> bool:
            """Return False if we've seen this key before."""
            if dedup_key in seen_keys:
                return False
            seen_keys.add(dedup_key)
            return True

        scraper = SkipDedupScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            duplicate_check=duplicate_check,
        )

        driver.run()

        # All 3 requests should be processed since SkipDeduplicationCheck bypasses
        # the duplicate_check callback
        assert len(results) == 3

    def test_skip_dedup_not_tracked_in_seen_keys(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """SkipDeduplicationCheck requests shall not be tracked in seen keys."""

        class MixedDedupScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            def parse(self, response: Response):
                # First: skip dedup request
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",
                    ),
                    continuation="parse_page",
                    deduplication_key=SkipDeduplicationCheck(),
                )
                # Second: normal request to same URL (should be processed)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",
                    ),
                    continuation="parse_page",
                )
                # Third: another normal request to same URL (should be skipped)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page1",
                    ),
                    continuation="parse_page",
                )

            def parse_page(self, response: Response):
                yield ParsedData(data={"url": response.url})

        seen_keys: set[str] = set()

        def duplicate_check(dedup_key: str) -> bool:
            if dedup_key in seen_keys:
                return False
            seen_keys.add(dedup_key)
            return True

        scraper = MixedDedupScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            duplicate_check=duplicate_check,
        )

        driver.run()

        # Should process: skip-dedup (1) + first normal (1) = 2 total
        # Third request is deduplicated by the callback
        assert len(results) == 2

    def test_skip_dedup_isinstance_check(self) -> None:
        """SkipDeduplicationCheck shall be identifiable via isinstance."""
        skip = SkipDeduplicationCheck()
        assert isinstance(skip, SkipDeduplicationCheck)

        # Regular dedup keys should not match
        assert not isinstance("some-key", SkipDeduplicationCheck)
        assert not isinstance(None, SkipDeduplicationCheck)
