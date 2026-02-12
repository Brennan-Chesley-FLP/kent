"""Tests for speculative entry functions and related functionality.

This test module verifies:
- The @entry(speculative=True) decorator attaches metadata correctly
- Speculative entry functions automatically set is_speculative=True on requests
- BaseScraper.list_speculators() discovers speculative entries
- Default values are applied correctly
- The old @speculate decorator still works for backward compatibility
"""

from collections.abc import Generator
from datetime import date
from unittest.mock import MagicMock

import pytest

from kent.common.decorators import (
    EntryMetadata,
    SpeculateMetadata,
    entry,
    get_entry_metadata,
    get_speculate_metadata,
    is_speculate,
    speculate,
    step,
)
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
    ParsedData,
    Response,
    ScraperYield,
)
from kent.driver.sync_driver import SyncDriver


class TestSpeculateDecorator:
    """Test the old @speculate decorator still works."""

    def test_speculate_basic_decorator(self):
        """Test that @speculate decorator attaches metadata."""

        @speculate
        def fetch_case(self, case_id: int) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/case/{case_id}"
                ),
                continuation="parse_case",
            )

        # Check metadata is attached
        metadata = get_speculate_metadata(fetch_case)
        assert metadata is not None
        assert isinstance(metadata, SpeculateMetadata)
        assert metadata.highest_observed == 1  # default
        assert metadata.largest_observed_gap == 10  # default
        assert metadata.observation_date is None  # default

    def test_speculate_with_parameters(self):
        """Test @speculate decorator with custom parameters."""
        obs_date = date(2024, 1, 15)

        @speculate(
            highest_observed=500,
            largest_observed_gap=20,
            observation_date=obs_date,
        )
        def fetch_case(self, case_id: int) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/case/{case_id}"
                ),
                continuation="parse_case",
            )

        metadata = get_speculate_metadata(fetch_case)
        assert metadata is not None
        assert metadata.highest_observed == 500
        assert metadata.largest_observed_gap == 20
        assert metadata.observation_date == obs_date

    def test_speculate_sets_is_speculative_true(self):
        """Test that @speculate automatically sets is_speculative=True."""

        class DummyScraper:
            @speculate
            def fetch_case(self, case_id: int) -> NavigatingRequest:
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

        scraper = DummyScraper()
        request = scraper.fetch_case(123)

        assert isinstance(request, NavigatingRequest)
        assert request.is_speculative is True
        assert request.request.url == "/case/123"

    def test_speculate_validates_return_type(self):
        """Test that @speculate raises TypeError if function doesn't return BaseRequest."""

        @speculate
        def bad_function(self, case_id: int) -> str:
            return f"/case/{case_id}"  # Returns string, not BaseRequest

        class DummyScraper:
            fetch_case = bad_function

        scraper = DummyScraper()
        with pytest.raises(TypeError, match="must return a BaseRequest"):
            scraper.fetch_case(123)

    def test_is_speculate_helper(self):
        """Test is_speculate() helper function."""

        @speculate
        def fetch_case(self, case_id: int) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/case/{case_id}"
                ),
                continuation="parse_case",
            )

        def normal_function(self):
            pass

        assert is_speculate(fetch_case) is True
        assert is_speculate(normal_function) is False


class TestEntrySpeculative:
    """Test @entry(speculative=True) decorator for speculative functions."""

    def test_entry_speculative_attaches_metadata(self):
        """Test that @entry(speculative=True) attaches EntryMetadata."""

        @entry(
            dict,
            speculative=True,
            highest_observed=100,
            largest_observed_gap=15,
        )
        def fetch_case(self, case_id: int) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/case/{case_id}"
                ),
                continuation="parse_case",
            )

        metadata = get_entry_metadata(fetch_case)
        assert metadata is not None
        assert isinstance(metadata, EntryMetadata)
        assert metadata.speculative is True
        assert metadata.highest_observed == 100
        assert metadata.largest_observed_gap == 15

    def test_entry_speculative_defaults(self):
        """Test @entry(speculative=True) with default values."""

        @entry(dict, speculative=True)
        def fetch_item(self, item_id: int) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/item/{item_id}"
                ),
                continuation="parse_item",
            )

        metadata = get_entry_metadata(fetch_item)
        assert metadata is not None
        assert metadata.speculative is True
        assert metadata.highest_observed == 1
        assert metadata.largest_observed_gap == 10


class TestListSpeculators:
    """Test BaseScraper.list_speculators() method."""

    def test_list_speculators_empty(self):
        """Test list_speculators() on a scraper with no speculative functions."""

        class EmptyScraper(BaseScraper[dict]):
            @entry(dict)
            def get_entry(self):
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        speculators = EmptyScraper.list_speculators()
        assert speculators == []

    def test_list_speculators_single(self):
        """Test list_speculators() with one speculative entry function."""

        class SingleSpecScraper(BaseScraper[dict]):
            @entry(
                dict,
                speculative=True,
                highest_observed=100,
                largest_observed_gap=15,
            )
            def fetch_case(self, case_id: int) -> NavigatingRequest:
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

            @entry(dict)
            def get_entry(self):
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        speculators = SingleSpecScraper.list_speculators()
        assert len(speculators) == 1

        name, highest, obs_date, gap = speculators[0]
        assert name == "fetch_case"
        assert highest == 100
        assert obs_date is None
        assert gap == 15

    def test_list_speculators_multiple(self):
        """Test list_speculators() with multiple speculative entry functions."""
        obs_date_1 = date(2024, 1, 10)
        obs_date_2 = date(2024, 2, 15)

        class MultiSpecScraper(BaseScraper[dict]):
            @entry(
                dict,
                speculative=True,
                highest_observed=500,
                largest_observed_gap=20,
                observation_date=obs_date_1,
            )
            def fetch_case(self, case_id: int) -> NavigatingRequest:
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

            @entry(
                dict,
                speculative=True,
                highest_observed=1000,
                largest_observed_gap=50,
                observation_date=obs_date_2,
            )
            def fetch_docket(self, docket_id: int) -> NavigatingRequest:
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/docket/{docket_id}"
                    ),
                    continuation="parse_docket",
                )

            @entry(dict)
            def get_entry(self):
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        speculators = MultiSpecScraper.list_speculators()
        assert len(speculators) == 2

        # Sort by name for deterministic testing
        speculators_dict = {name: (h, d, g) for name, h, d, g in speculators}

        assert "fetch_case" in speculators_dict
        assert speculators_dict["fetch_case"] == (500, obs_date_1, 20)

        assert "fetch_docket" in speculators_dict
        assert speculators_dict["fetch_docket"] == (1000, obs_date_2, 50)

    def test_list_speculators_defaults(self):
        """Test list_speculators() with default metadata values."""

        class DefaultsScraper(BaseScraper[dict]):
            @entry(dict, speculative=True)
            def fetch_item(self, item_id: int) -> NavigatingRequest:
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/item/{item_id}"
                    ),
                    continuation="parse_item",
                )

            @entry(dict)
            def get_entry(self):
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        speculators = DefaultsScraper.list_speculators()
        assert len(speculators) == 1

        name, highest, obs_date, gap = speculators[0]
        assert name == "fetch_item"
        assert highest == 1  # default
        assert obs_date is None  # default
        assert gap == 10  # default


class TestSpeculateMetadata:
    """Test SpeculateMetadata dataclass."""

    def test_metadata_defaults(self):
        """Test SpeculateMetadata default values."""
        metadata = SpeculateMetadata()
        assert metadata.observation_date is None
        assert metadata.highest_observed == 1
        assert metadata.largest_observed_gap == 10

    def test_metadata_custom_values(self):
        """Test SpeculateMetadata with custom values."""
        obs_date = date(2024, 3, 1)
        metadata = SpeculateMetadata(
            observation_date=obs_date,
            highest_observed=999,
            largest_observed_gap=42,
        )
        assert metadata.observation_date == obs_date
        assert metadata.highest_observed == 999
        assert metadata.largest_observed_gap == 42


class TestIsSpeculativeField:
    """Test the is_speculative field on BaseRequest."""

    def test_is_speculative_defaults_to_false(self):
        """Test that is_speculative defaults to False on NavigatingRequest."""
        req = NavigatingRequest(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="parse",
        )
        assert req.is_speculative is False

    def test_is_speculative_can_be_set_true(self):
        """Test that is_speculative can be explicitly set to True."""
        req = NavigatingRequest(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="parse",
            is_speculative=True,
        )
        assert req.is_speculative is True

    def test_is_speculative_preserved_through_decorator(self):
        """Test that @speculate decorator preserves is_speculative=True."""

        class TestScraper:
            @speculate
            def fetch_record(self, record_id: int) -> NavigatingRequest:
                # Create request without is_speculative
                return NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/record/{record_id}"
                    ),
                    continuation="parse_record",
                )

        scraper = TestScraper()
        request = scraper.fetch_record(456)

        # Decorator should have set is_speculative=True
        assert request.is_speculative is True


# =============================================================================
# Integration Tests for Driver Speculation Support
# =============================================================================


class SpeculationTestScraper(BaseScraper[dict]):
    """Test scraper with @entry(speculative=True) for driver integration tests."""

    def __init__(self) -> None:
        self.processed_ids: list[int] = []

    @entry(dict, speculative=True, highest_observed=5, largest_observed_gap=2)
    def fetch_case(self, case_id: int) -> NavigatingRequest:
        """Speculative request factory."""
        return NavigatingRequest(
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
        """Parse a case page."""
        case_id = int(response.url.split("/")[-1])
        self.processed_ids.append(case_id)
        yield ParsedData({"case_id": case_id})

    @entry(dict)
    def get_entry(self) -> Generator[NavigatingRequest, None, None]:
        # No entry requests - speculation seeds the queue
        return
        yield  # Make this a generator


class TestSyncDriverSpeculationDiscovery:
    """Test SyncDriver discovers and initializes speculation state."""

    def test_driver_discovers_speculate_functions(self):
        """Test that _discover_speculate_functions finds @entry(speculative=True) methods."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        assert "fetch_case" in state
        assert state["fetch_case"].func_name == "fetch_case"
        assert state["fetch_case"].metadata.highest_observed == 5
        assert state["fetch_case"].metadata.largest_observed_gap == 2

    def test_driver_seeds_speculative_queue(self):
        """Test that speculation is seeded to the queue with correct range."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        # Discover and seed
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        # Queue should have 5 requests (IDs 1-5 from highest_observed=5)
        assert len(driver.request_queue) == 5

        # Check all requests are speculative
        for _priority, _counter, request in driver.request_queue:
            assert request.is_speculative is True

    def test_driver_uses_definite_range_from_config(self):
        """Test that SpeculateFunctionConfig.definite_range overrides defaults."""
        from kent.common.searchable import (
            SpeculateFunctionConfig,
        )

        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()

        # Override the config with a definite_range
        driver._speculation_state[
            "fetch_case"
        ].config = SpeculateFunctionConfig(definite_range=(10, 15))
        driver._seed_speculative_queue()

        # Queue should have 6 requests (IDs 10-15)
        assert len(driver.request_queue) == 6

        # Verify URL IDs
        urls = [req.request.url for _p, _c, req in driver.request_queue]
        expected_ids = set(range(10, 16))
        actual_ids = {int(url.split("/")[-1]) for url in urls}
        assert actual_ids == expected_ids


class TestSyncDriverSpeculationTracking:
    """Test SyncDriver tracks speculation state correctly."""

    def test_tracking_updates_highest_successful_id(self):
        """Test that highest_successful_id is updated on success."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        # Setup speculation state
        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        # Create a request with speculation tracking fields
        request = scraper.fetch_case(42).speculative("fetch_case", 42)

        # Create a 200 response
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/42",
            request=request,
        )

        # Track outcome
        driver._track_speculation_outcome(request, response)

        assert spec_state.highest_successful_id == 42
        assert spec_state.consecutive_failures == 0

    def test_tracking_increments_consecutive_failures(self):
        """Test that consecutive_failures increments on failure beyond highest."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        # Setup speculation state with highest_successful_id = 40
        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]
        spec_state.highest_successful_id = 40

        # Create a request for ID 42 (beyond highest)
        request = scraper.fetch_case(42).speculative("fetch_case", 42)

        # Create a 404 response
        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/42",
            request=request,
        )

        # Track outcome
        driver._track_speculation_outcome(request, response)

        assert spec_state.highest_successful_id == 40  # unchanged
        assert spec_state.consecutive_failures == 1

    def test_tracking_stops_after_plus_failures(self):
        """Test that speculation stops after plus consecutive failures."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        # Setup speculation state
        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]
        spec_state.highest_successful_id = 40
        spec_state.consecutive_failures = 1  # Already 1 failure

        # Create a request for ID 42
        request = scraper.fetch_case(42)
        request = request.speculative("fetch_case", 42)

        # Create a 404 response
        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/42",
            request=request,
        )

        # Track outcome - this should be the 2nd failure
        # With largest_observed_gap=2, this should stop speculation
        driver._track_speculation_outcome(request, response)

        assert spec_state.consecutive_failures == 2
        assert spec_state.stopped is True


# =============================================================================
# End-to-End Integration Tests with HTTP Mocking
# =============================================================================


def create_mock_response(status_code: int, url: str) -> MagicMock:
    """Create a mock HTTP response."""
    response = MagicMock()
    response.status_code = status_code
    response.headers = {}
    response.content = b"<html><body>Test</body></html>"
    response.text = "<html><body>Test</body></html>"
    return response


class EndToEndSpeculationScraper(BaseScraper[dict]):
    """Scraper for end-to-end integration testing."""

    def __init__(self) -> None:
        self.processed_ids: list[int] = []

    @entry(dict, speculative=True, highest_observed=5, largest_observed_gap=3)
    def fetch_case(self, case_id: int) -> NavigatingRequest:
        """Speculative request for case IDs."""
        return NavigatingRequest(
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
        """Parse a case page and record the ID (only for successful responses)."""
        # Only track successful responses
        if 200 <= response.status_code < 300:
            case_id = int(response.url.split("/")[-1])
            self.processed_ids.append(case_id)
            yield ParsedData({"case_id": case_id})

    @entry(dict)
    def get_entry(self) -> Generator[NavigatingRequest, None, None]:
        # No entry requests - speculation seeds the queue
        return
        yield  # Make this a generator


class TestSyncDriverEndToEndSpeculation:
    """End-to-end tests for SyncDriver with @entry(speculative=True) functions."""

    def test_stops_after_consecutive_failures(self):
        """Test that driver stops after largest_observed_gap consecutive failures."""
        scraper = EndToEndSpeculationScraper()
        collected_data: list[dict] = []

        def collect(data: dict) -> None:
            collected_data.append(data)

        # IDs 1-5 succeed, then 6+ fail
        def mock_request(**kwargs) -> MagicMock:
            url = kwargs["url"]
            case_id = int(url.split("/")[-1])
            if case_id <= 5:
                return create_mock_response(200, url)
            else:
                return create_mock_response(404, url)

        driver = SyncDriver(scraper, on_data=collect)
        driver.request_manager._client = MagicMock()
        driver.request_manager._client.request.side_effect = mock_request

        driver.run()

        # Should process IDs 1-5 successfully
        assert set(scraper.processed_ids) == {1, 2, 3, 4, 5}

        # The speculation extends as successful IDs approach the ceiling:
        # - Initial: IDs 1-5 seeded (ceiling=5, plus=3)
        # - ID 2 succeeds: 2 >= (5-3) triggers extension to IDs 6-8 (ceiling=8)
        # - ID 5 succeeds: 5 >= (8-3) triggers extension to IDs 9-11 (ceiling=11)
        # - IDs 6-8 fail: consecutive_failures reaches 3, stopped=True
        # - IDs 9-11 are still processed but no further extension
        total_calls = driver.request_manager._client.request.call_count
        assert total_calls == 11  # 5 successes + 6 failures before stopping

    def test_resets_failure_count_on_success(self):
        """Test that successful request resets consecutive failure count."""
        scraper = EndToEndSpeculationScraper()
        collected_data: list[dict] = []

        def collect(data: dict) -> None:
            collected_data.append(data)

        # IDs 1-3 succeed, 4-5 fail, 6 succeeds, 7-9 fail
        def mock_request(**kwargs) -> MagicMock:
            url = kwargs["url"]
            case_id = int(url.split("/")[-1])
            if case_id in {1, 2, 3, 6}:
                return create_mock_response(200, url)
            else:
                return create_mock_response(404, url)

        driver = SyncDriver(scraper, on_data=collect)
        driver.request_manager._client = MagicMock()
        driver.request_manager._client.request.side_effect = mock_request

        driver.run()

        # Should process 1, 2, 3, and 6 (success after gap)
        assert set(scraper.processed_ids) == {1, 2, 3, 6}

    def test_uses_definite_range_config(self):
        """Test that SpeculateFunctionConfig.definite_range overrides defaults."""
        from kent.common.searchable import (
            SpeculateFunctionConfig,
        )

        scraper = EndToEndSpeculationScraper()
        driver = SyncDriver(scraper)

        # Discover state and override config
        driver._speculation_state = driver._discover_speculate_functions()
        driver._speculation_state[
            "fetch_case"
        ].config = SpeculateFunctionConfig(definite_range=(10, 15))
        driver._seed_speculative_queue()

        # Queue should have 6 requests (IDs 10-15)
        assert len(driver.request_queue) == 6

        # Verify URL IDs
        urls = [req.request.url for _p, _c, req in driver.request_queue]
        expected_ids = set(range(10, 16))
        actual_ids = {int(url.split("/")[-1]) for url in urls}
        assert actual_ids == expected_ids


# Note: AsyncDriver uses the same _track_speculation_outcome and _extend_speculation
# methods as SyncDriver. The SyncDriver end-to-end tests above verify the speculation
# logic works correctly. AsyncDriver-specific tests would be redundant since the
# speculation tracking logic is identical.
