"""Tests for speculative entry functions and related functionality.

This test module verifies:
- The @entry(speculative=SimpleSpeculation(...)) decorator attaches metadata correctly
- Speculative entry functions automatically set is_speculative=True on requests
- BaseScraper.list_speculative_entries() discovers speculative entries
- Default values are applied correctly
- The old @speculate decorator still works for backward compatibility
- YearlySpeculation discovery, seeding, tracking, and end-to-end behavior
"""

from collections.abc import Generator
from datetime import date, timedelta
from unittest.mock import MagicMock

import pytest
from freezegun import freeze_time

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
from kent.common.speculation_types import (
    SimpleSpeculation,
    YearlySpeculation,
    YearPartition,
)
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.driver.sync_driver import SyncDriver


class TestSpeculateDecorator:
    """Test the old @speculate decorator still works."""

    def test_speculate_basic_decorator(self):
        """Test that @speculate decorator attaches metadata."""

        @speculate
        def fetch_case(self, case_id: int) -> Request:
            return Request(
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
        def fetch_case(self, case_id: int) -> Request:
            return Request(
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
            def fetch_case(self, case_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

        scraper = DummyScraper()
        request = scraper.fetch_case(123)

        assert isinstance(request, Request)
        assert request.is_speculative is True
        assert request.request.url == "/case/123"

    def test_speculate_validates_return_type(self):
        """Test that @speculate raises TypeError if function doesn't return a Request."""

        @speculate
        def bad_function(self, case_id: int) -> str:
            return f"/case/{case_id}"  # Returns string, not Request

        class DummyScraper:
            fetch_case = bad_function

        scraper = DummyScraper()
        with pytest.raises(TypeError, match="must return a Request"):
            scraper.fetch_case(123)

    def test_is_speculate_helper(self):
        """Test is_speculate() helper function."""

        @speculate
        def fetch_case(self, case_id: int) -> Request:
            return Request(
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
    """Test @entry(speculative=SimpleSpeculation(...)) decorator."""

    def test_entry_speculative_attaches_metadata(self):
        """Test that @entry(speculative=SimpleSpeculation(...)) attaches EntryMetadata."""

        @entry(
            dict,
            speculative=SimpleSpeculation(
                highest_observed=100,
                largest_observed_gap=15,
            ),
        )
        def fetch_case(self, case_id: int) -> Request:
            return Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/case/{case_id}"
                ),
                continuation="parse_case",
            )

        metadata = get_entry_metadata(fetch_case)
        assert metadata is not None
        assert isinstance(metadata, EntryMetadata)
        assert metadata.speculative is True
        assert isinstance(metadata.speculation, SimpleSpeculation)
        assert metadata.speculation.highest_observed == 100
        assert metadata.speculation.largest_observed_gap == 15

    def test_entry_speculative_defaults(self):
        """Test @entry(speculative=SimpleSpeculation()) with default values."""

        @entry(dict, speculative=SimpleSpeculation())
        def fetch_item(self, item_id: int) -> Request:
            return Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET, url=f"/item/{item_id}"
                ),
                continuation="parse_item",
            )

        metadata = get_entry_metadata(fetch_item)
        assert metadata is not None
        assert metadata.speculative is True
        assert metadata.speculation.highest_observed == 1
        assert metadata.speculation.largest_observed_gap == 10


class TestListSpeculativeEntries:
    """Test BaseScraper.list_speculative_entries() method."""

    def test_list_speculative_entries_empty(self):
        """Test list_speculative_entries() on a scraper with no speculative functions."""

        class EmptyScraper(BaseScraper[dict]):
            @entry(dict)
            def get_entry(self):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        entries = EmptyScraper.list_speculative_entries()
        assert entries == []

    def test_list_speculative_entries_single(self):
        """Test list_speculative_entries() with one speculative entry function."""

        class SingleSpecScraper(BaseScraper[dict]):
            @entry(
                dict,
                speculative=SimpleSpeculation(
                    highest_observed=100,
                    largest_observed_gap=15,
                ),
            )
            def fetch_case(self, case_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

            @entry(dict)
            def get_entry(self):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        entries = SingleSpecScraper.list_speculative_entries()
        assert len(entries) == 1

        info = entries[0]
        assert info.name == "fetch_case"
        assert info.speculative is True
        assert isinstance(info.speculation, SimpleSpeculation)
        assert info.speculation.highest_observed == 100
        assert info.speculation.largest_observed_gap == 15

    def test_list_speculative_entries_multiple(self):
        """Test list_speculative_entries() with multiple speculative entry functions."""
        obs_date_1 = date(2024, 1, 10)

        class MultiSpecScraper(BaseScraper[dict]):
            @entry(
                dict,
                speculative=SimpleSpeculation(
                    highest_observed=500,
                    largest_observed_gap=20,
                    observation_date=obs_date_1,
                ),
            )
            def fetch_case(self, case_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/case/{case_id}"
                    ),
                    continuation="parse_case",
                )

            @entry(
                dict,
                speculative=SimpleSpeculation(
                    highest_observed=1000,
                    largest_observed_gap=50,
                ),
            )
            def fetch_docket(self, docket_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/docket/{docket_id}"
                    ),
                    continuation="parse_docket",
                )

            @entry(dict)
            def get_entry(self):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        entries = MultiSpecScraper.list_speculative_entries()
        assert len(entries) == 2

        by_name = {e.name: e for e in entries}
        assert by_name["fetch_case"].speculation.highest_observed == 500
        assert by_name["fetch_case"].speculation.largest_observed_gap == 20
        assert by_name["fetch_docket"].speculation.highest_observed == 1000
        assert by_name["fetch_docket"].speculation.largest_observed_gap == 50

    def test_list_speculative_entries_defaults(self):
        """Test list_speculative_entries() with default metadata values."""

        class DefaultsScraper(BaseScraper[dict]):
            @entry(dict, speculative=SimpleSpeculation())
            def fetch_item(self, item_id: int) -> Request:
                return Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url=f"/item/{item_id}"
                    ),
                    continuation="parse_item",
                )

            @entry(dict)
            def get_entry(self):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET, url="/start"
                    ),
                    continuation="parse",
                )

        entries = DefaultsScraper.list_speculative_entries()
        assert len(entries) == 1

        info = entries[0]
        assert info.name == "fetch_item"
        assert info.speculation.highest_observed == 1
        assert info.speculation.largest_observed_gap == 10


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
        """Test that is_speculative defaults to False on Request."""
        req = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="parse",
        )
        assert req.is_speculative is False

    def test_is_speculative_can_be_set_true(self):
        """Test that is_speculative can be explicitly set to True."""
        req = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="parse",
            is_speculative=True,
        )
        assert req.is_speculative is True

    def test_is_speculative_preserved_through_decorator(self):
        """Test that @speculate decorator preserves is_speculative=True."""

        class TestScraper:
            @speculate
            def fetch_record(self, record_id: int) -> Request:
                # Create request without is_speculative
                return Request(
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
    """Test scraper with @entry(speculative=SimpleSpeculation(...)) for driver integration tests."""

    def __init__(self) -> None:
        self.processed_ids: list[int] = []

    @entry(
        dict,
        speculative=SimpleSpeculation(
            highest_observed=5,
            largest_observed_gap=2,
        ),
    )
    def fetch_case(self, case_id: int) -> Request:
        """Speculative request factory."""
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
        """Parse a case page."""
        case_id = int(response.url.split("/")[-1])
        self.processed_ids.append(case_id)
        yield ParsedData({"case_id": case_id})

    @entry(dict)
    def get_entry(self) -> Generator[Request, None, None]:
        # No entry requests - speculation seeds the queue
        return
        yield  # Make this a generator


class TestSyncDriverSpeculationDiscovery:
    """Test SyncDriver discovers and initializes speculation state."""

    def test_driver_discovers_speculate_functions(self):
        """Test that _discover_speculate_functions finds @entry(speculative=...) methods."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        assert "fetch_case" in state
        assert state["fetch_case"].func_name == "fetch_case"
        assert isinstance(state["fetch_case"].speculation, SimpleSpeculation)
        assert state["fetch_case"].speculation.highest_observed == 5
        assert state["fetch_case"].speculation.largest_observed_gap == 2

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
# YearlySpeculation Integration Tests
# =============================================================================


class YearlySpeculationTestScraper(BaseScraper[dict]):
    """Test scraper with YearlySpeculation for integration tests."""

    def __init__(self) -> None:
        self.processed: list[tuple[int, int]] = []

    @entry(
        dict,
        speculative=YearlySpeculation(
            backfill=(
                YearPartition(year=2023, number=(1, 5), frozen=True),
                YearPartition(year=2024, number=(1, 3), frozen=False),
            ),
            trailing_period=timedelta(days=60),
            largest_observed_gap=2,
        ),
    )
    def fetch_docket(self, year: int, number: int) -> Request:
        """Speculative request factory for year-partitioned dockets."""
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/docket/{year}/{number}",
            ),
            continuation="parse_docket",
            is_speculative=True,
        )

    @step
    def parse_docket(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        """Parse a docket page."""
        parts = response.url.rstrip("/").split("/")
        year, number = int(parts[-2]), int(parts[-1])
        self.processed.append((year, number))
        yield ParsedData({"year": year, "number": number})

    @entry(dict)
    def get_entry(self) -> Generator[Request, None, None]:
        return
        yield


class TestYearlySpeculationDiscovery:
    """Test SyncDriver discovers YearlySpeculation partitions."""

    def test_discovers_yearly_partitions(self):
        """Test that discovery creates per-year SpeculationState entries."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        # Should have at least the 2023 and 2024 backfill partitions
        assert "fetch_docket:2023" in state
        assert "fetch_docket:2024" in state

        # 2023 is frozen
        s2023 = state["fetch_docket:2023"]
        assert s2023.year == 2023
        assert s2023.frozen is True
        assert s2023.base_func_name == "fetch_docket"
        assert s2023.config.definite_range == (1, 5)

        # 2024 is not frozen
        s2024 = state["fetch_docket:2024"]
        assert s2024.year == 2024
        assert s2024.frozen is False
        assert s2024.config.definite_range == (1, 3)

    def test_rollover_creates_current_year(self):
        """Test that current year is auto-created if not in backfill."""
        from datetime import date as date_cls

        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        current_year = date_cls.today().year
        current_key = f"fetch_docket:{current_year}"

        # Current year should be created unless it's already in backfill
        if current_year not in (2023, 2024):
            assert current_key in state
            s_current = state[current_key]
            assert s_current.year == current_year
            assert s_current.frozen is False
            assert s_current.config.definite_range == (
                1,
                2,
            )  # (1, largest_observed_gap)


class TestYearlySpeculationSeeding:
    """Test SyncDriver seeds YearlySpeculation with year+number calls."""

    def test_seeds_frozen_partition(self):
        """Test that frozen partitions are seeded and then stopped."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        # Only keep the 2023 frozen partition for isolated testing
        driver._speculation_state = {
            k: v
            for k, v in driver._speculation_state.items()
            if k == "fetch_docket:2023"
        }
        driver._seed_speculative_queue()

        # Should have 5 requests (IDs 1-5)
        assert len(driver.request_queue) == 5

        # Verify URLs contain year and number
        for _p, _c, req in driver.request_queue:
            assert "/docket/2023/" in req.request.url
            assert req.is_speculative is True

        # Frozen partition should be stopped after seeding
        assert driver._speculation_state["fetch_docket:2023"].stopped is True

    def test_seeds_non_frozen_partition(self):
        """Test that non-frozen partitions are seeded but not stopped."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        # Only keep the 2024 non-frozen partition
        driver._speculation_state = {
            k: v
            for k, v in driver._speculation_state.items()
            if k == "fetch_docket:2024"
        }
        driver._seed_speculative_queue()

        # Should have 3 requests (IDs 1-3)
        assert len(driver.request_queue) == 3

        # Non-frozen partition should NOT be stopped
        assert driver._speculation_state["fetch_docket:2024"].stopped is False

    def test_speculation_id_uses_composite_key(self):
        """Test that speculation_id uses func_name:year composite key."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        driver._speculation_state = {
            k: v
            for k, v in driver._speculation_state.items()
            if k == "fetch_docket:2024"
        }
        driver._seed_speculative_queue()

        for _p, _c, req in driver.request_queue:
            assert req.speculation_id is not None
            state_key, spec_id = req.speculation_id
            assert state_key == "fetch_docket:2024"
            assert isinstance(spec_id, int)


class TestYearlySpeculationFrozen:
    """Test that frozen partitions never extend."""

    def test_frozen_does_not_extend(self):
        """Test that _extend_speculation returns early for frozen partitions."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        s2023 = driver._speculation_state["fetch_docket:2023"]
        s2023.current_ceiling = 5
        s2023.highest_successful_id = 5  # At ceiling

        # Extension should do nothing
        initial_ceiling = s2023.current_ceiling
        driver._extend_speculation("fetch_docket:2023")
        assert s2023.current_ceiling == initial_ceiling


class TestYearlySpeculationTracking:
    """Test tracking with composite keys for YearlySpeculation."""

    def test_tracking_with_composite_key(self):
        """Test that tracking works with func_name:year composite keys."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        s2024 = driver._speculation_state["fetch_docket:2024"]

        # Create a speculative request with composite key
        request = scraper.fetch_docket(2024, 2).speculative(
            "fetch_docket:2024", 2
        )

        # Success response
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="https://example.com/docket/2024/2",
            request=request,
        )

        driver._track_speculation_outcome(request, response)
        assert s2024.highest_successful_id == 2
        assert s2024.consecutive_failures == 0

    def test_tracking_failure_with_composite_key(self):
        """Test tracking failure with composite key."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        s2024 = driver._speculation_state["fetch_docket:2024"]
        s2024.highest_successful_id = 2

        # Failure for ID beyond highest
        request = scraper.fetch_docket(2024, 4).speculative(
            "fetch_docket:2024", 4
        )
        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/docket/2024/4",
            request=request,
        )

        driver._track_speculation_outcome(request, response)
        assert s2024.consecutive_failures == 1

    def test_yearly_stops_after_gap_failures(self):
        """Test that yearly speculation stops after largest_observed_gap failures."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        s2024 = driver._speculation_state["fetch_docket:2024"]
        s2024.highest_successful_id = 2
        s2024.consecutive_failures = 1  # One failure already

        request = scraper.fetch_docket(2024, 4).speculative(
            "fetch_docket:2024", 4
        )
        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/docket/2024/4",
            request=request,
        )

        driver._track_speculation_outcome(request, response)
        assert s2024.consecutive_failures == 2
        assert s2024.stopped is True


class TestYearlySpeculationSchema:
    """Test schema generation for YearlySpeculation entries."""

    def test_yearly_schema(self):
        """Test that schema() produces correct format for YearlySpeculation."""
        schema = YearlySpeculationTestScraper.schema()
        entry = schema["entries"]["fetch_docket"]

        assert entry["speculative"] is True
        assert entry["largest_observed_gap"] == 2
        assert entry["trailing_period_days"] == 60

        props = entry["parameters"]["properties"]
        assert props["year"] == {"type": "integer"}
        assert props["number"] == {
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 2,
            "maxItems": 2,
        }
        assert props["frozen"] == {"type": "boolean", "default": False}

        required = entry["parameters"]["required"]
        assert "year" in required
        assert "number" in required
        # frozen is optional
        assert "frozen" not in required


class TestYearlySpeculationValidateParams:
    """Test validate_params for YearlySpeculation entries."""

    def test_validates_yearly_params(self):
        """Test that validate_params handles YearlySpeculation range format."""
        meta = get_entry_metadata(YearlySpeculationTestScraper.fetch_docket)
        result = meta.validate_params({"year": 2025, "number": [1, 100]})
        assert result == {"year": 2025, "number": (1, 100), "frozen": False}

    def test_validates_yearly_params_with_frozen(self):
        """Test that validate_params handles frozen flag."""
        meta = get_entry_metadata(YearlySpeculationTestScraper.fetch_docket)
        result = meta.validate_params(
            {"year": 2023, "number": [1, 50], "frozen": True}
        )
        assert result == {"year": 2023, "number": (1, 50), "frozen": True}

    def test_validates_yearly_params_missing_year_raises(self):
        """Test that missing year raises."""
        meta = get_entry_metadata(YearlySpeculationTestScraper.fetch_docket)
        with pytest.raises(
            ValueError, match="Missing required parameter.*year"
        ):
            meta.validate_params({"number": [1, 50]})


class TestYearlySpeculationInitialSeed:
    """Test initial_seed with YearlySpeculation entries."""

    def test_yearly_initial_seed_stores_overrides(self):
        """Test that speculative yearly entries store overrides."""
        scraper = YearlySpeculationTestScraper()
        requests = list(
            scraper.initial_seed(
                [{"fetch_docket": {"year": 2025, "number": [1, 100]}}]
            )
        )
        assert len(requests) == 0
        assert "fetch_docket" in scraper._speculation_overrides
        assert scraper._speculation_overrides["fetch_docket"] == [
            {"year": 2025, "number": (1, 100), "frozen": False}
        ]


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

    @entry(
        dict,
        speculative=SimpleSpeculation(
            highest_observed=5,
            largest_observed_gap=3,
        ),
    )
    def fetch_case(self, case_id: int) -> Request:
        """Speculative request for case IDs."""
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
        """Parse a case page and record the ID (only for successful responses)."""
        # Only track successful responses
        if 200 <= response.status_code < 300:
            case_id = int(response.url.split("/")[-1])
            self.processed_ids.append(case_id)
            yield ParsedData({"case_id": case_id})

    @entry(dict)
    def get_entry(self) -> Generator[Request, None, None]:
        # No entry requests - speculation seeds the queue
        return
        yield  # Make this a generator


class TestSyncDriverEndToEndSpeculation:
    """End-to-end tests for SyncDriver with @entry(speculative=SimpleSpeculation(...)) functions."""

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


# =============================================================================
# _extend_speculation() Unit Tests
# =============================================================================


class TestExtendSpeculation:
    """Test _extend_speculation() in isolation."""

    def test_extends_when_near_ceiling(self):
        """Verify extension adds `plus` new IDs when highest_successful_id approaches ceiling."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        # Seed so queue is populated and ceiling set
        driver._seed_speculative_queue()
        initial_queue_size = len(driver.request_queue)
        assert spec_state.current_ceiling == 5  # highest_observed=5

        # Set highest_successful_id near ceiling (5 >= 5 - 2 = 3)
        spec_state.highest_successful_id = 5
        spec_state.consecutive_failures = 0

        driver._extend_speculation("fetch_case")

        # Should have added `largest_observed_gap` (2) new IDs: 6, 7
        assert len(driver.request_queue) == initial_queue_size + 2
        assert spec_state.current_ceiling == 7

    def test_does_not_extend_when_far_from_ceiling(self):
        """Verify no extension when highest_successful_id is well below ceiling."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        driver._seed_speculative_queue()
        initial_queue_size = len(driver.request_queue)

        # highest_successful_id=1, ceiling=5, plus=2 → 1 < (5-2)=3, no extension
        spec_state.highest_successful_id = 1
        spec_state.consecutive_failures = 0

        driver._extend_speculation("fetch_case")

        assert len(driver.request_queue) == initial_queue_size
        assert spec_state.current_ceiling == 5

    def test_config_plus_overrides_largest_observed_gap(self):
        """Verify config.plus is used instead of speculation.largest_observed_gap."""
        from kent.common.searchable import SpeculateFunctionConfig

        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        driver._seed_speculative_queue()
        initial_queue_size = len(driver.request_queue)

        # Override plus to 4 (larger than largest_observed_gap=2)
        spec_state.config = SpeculateFunctionConfig(plus=4)
        spec_state.highest_successful_id = 5
        spec_state.current_ceiling = 5
        spec_state.consecutive_failures = 0

        driver._extend_speculation("fetch_case")

        # Should add 4 new IDs (6, 7, 8, 9) using config.plus=4
        assert len(driver.request_queue) == initial_queue_size + 4
        assert spec_state.current_ceiling == 9

    def test_stopped_partition_not_extended(self):
        """Verify _extend_speculation is a no-op for stopped partitions."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        driver._seed_speculative_queue()
        initial_queue_size = len(driver.request_queue)

        spec_state.highest_successful_id = 5
        spec_state.stopped = True

        driver._extend_speculation("fetch_case")

        assert len(driver.request_queue) == initial_queue_size
        assert spec_state.current_ceiling == 5

    def test_extends_yearly_speculation_with_composite_key(self):
        """Verify extension works for YearlySpeculation with year parameter."""
        scraper = YearlySpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        # Isolate the 2024 non-frozen partition
        driver._speculation_state = {
            k: v
            for k, v in driver._speculation_state.items()
            if k == "fetch_docket:2024"
        }
        driver._seed_speculative_queue()
        initial_queue_size = len(driver.request_queue)

        spec_state = driver._speculation_state["fetch_docket:2024"]
        # Set near ceiling: highest=3, ceiling=3, plus=2 → 3 >= (3-2)=1
        spec_state.highest_successful_id = 3
        spec_state.consecutive_failures = 0

        driver._extend_speculation("fetch_docket:2024")

        # Should add 2 new requests with year=2024
        assert len(driver.request_queue) == initial_queue_size + 2
        assert spec_state.current_ceiling == 5

        # Verify new requests have correct year in URL
        new_requests = sorted(driver.request_queue, key=lambda x: x[1])[-2:]
        for _p, _c, req in new_requests:
            assert "/docket/2024/" in req.request.url
            assert req.is_speculative is True


# =============================================================================
# fails_successfully() Soft 404 Detection Tests
# =============================================================================


class SoftFourOhFourScraper(BaseScraper[dict]):
    """Scraper that detects soft 404s in 200 responses."""

    def __init__(self) -> None:
        self.processed_ids: list[int] = []

    @entry(
        dict,
        speculative=SimpleSpeculation(
            highest_observed=5,
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
        self.processed_ids.append(case_id)
        yield ParsedData({"case_id": case_id})

    @entry(dict)
    def get_entry(self) -> Generator[Request, None, None]:
        return
        yield

    def fails_successfully(self, response: Response) -> bool:
        """Detect soft 404: pages containing 'No results found'."""
        return "No results found" not in response.text


class TestFailsSuccessfully:
    """Test fails_successfully() soft 404 detection."""

    def test_default_returns_true(self):
        """Default BaseScraper.fails_successfully() always returns True."""
        scraper = SpeculationTestScraper()
        response = Response(
            status_code=200,
            headers={},
            content=b"anything",
            text="anything",
            url="https://example.com/test",
            request=MagicMock(),
        )
        assert scraper.fails_successfully(response) is True

    def test_override_returns_false_treated_as_failure(self):
        """A 200 response where fails_successfully() returns False is treated as failure."""
        scraper = SoftFourOhFourScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        request = scraper.fetch_case(3).speculative("fetch_case", 3)
        # 200 response with soft 404 content
        response = Response(
            status_code=200,
            headers={},
            content=b"No results found",
            text="No results found",
            url="https://example.com/case/3",
            request=request,
        )

        driver._track_speculation_outcome(request, response)

        # Should be treated as a failure despite 200 status
        assert spec_state.highest_successful_id == 0
        assert spec_state.consecutive_failures == 1

    def test_end_to_end_soft_404_stops_speculation(self):
        """Full driver.run() with soft 404s causes speculation to stop."""
        scraper = SoftFourOhFourScraper()
        collected_data: list[dict] = []

        def collect(data: dict) -> None:
            collected_data.append(data)

        # IDs 1-3 return real content, 4+ return soft 404
        def mock_request(**kwargs) -> MagicMock:
            url = kwargs["url"]
            case_id = int(url.split("/")[-1])
            resp = MagicMock()
            resp.headers = {}
            if case_id <= 3:
                resp.status_code = 200
                resp.content = b"<html>Real case data</html>"
                resp.text = "<html>Real case data</html>"
            else:
                resp.status_code = 200  # Still 200!
                resp.content = b"No results found"
                resp.text = "No results found"
            return resp

        driver = SyncDriver(scraper, on_data=collect)
        driver.request_manager._client = MagicMock()
        driver.request_manager._client.request.side_effect = mock_request

        driver.run()

        # parse_case runs for ALL responses (soft 404 detection only
        # affects speculation state, not whether the continuation runs).
        # Trace:
        #   IDs 1-5 seeded (ceiling=5, plus=2)
        #   ID 3 succeeds → highest_successful_id=3 >= (5-2)=3 → extend to 6,7
        #   ID 4 soft 404 → consecutive_failures=1
        #   ID 5 soft 404 → consecutive_failures=2 >= plus=2 → stopped
        #   IDs 6,7 already queued → still processed, no further extension
        assert set(scraper.processed_ids) == {1, 2, 3, 4, 5, 6, 7}

        # Verify speculation stopped and soft 404s were detected
        spec_state = driver._speculation_state["fetch_case"]
        assert spec_state.stopped is True
        assert spec_state.highest_successful_id == 3
        # 4 failures total: IDs 4,5,6,7 all soft 404 beyond watermark
        # (stopped flag set at 2, but remaining queued items still tracked)
        assert spec_state.consecutive_failures == 4


# =============================================================================
# _track_speculation_outcome() Edge Cases
# =============================================================================


class TestTrackSpeculationEdgeCases:
    """Test edge cases in _track_speculation_outcome()."""

    def test_failure_below_watermark_ignored(self):
        """Failure for ID below highest_successful_id does not increment consecutive_failures."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]
        spec_state.highest_successful_id = 10

        # Failure for ID 5 (below watermark of 10)
        request = scraper.fetch_case(5).speculative("fetch_case", 5)
        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/5",
            request=request,
        )

        driver._track_speculation_outcome(request, response)

        assert spec_state.consecutive_failures == 0
        assert spec_state.highest_successful_id == 10

    def test_non_speculative_request_ignored(self):
        """Non-speculative requests are ignored by _track_speculation_outcome()."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]

        # Normal (non-speculative) request
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/1",
            ),
            continuation="parse_case",
        )
        assert request.is_speculative is False

        response = Response(
            status_code=404,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/1",
            request=request,
        )

        driver._track_speculation_outcome(request, response)

        # State unchanged
        assert spec_state.highest_successful_id == 0
        assert spec_state.consecutive_failures == 0

    def test_success_below_watermark_does_not_lower_it(self):
        """Success for ID below highest_successful_id does not lower the watermark."""
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)

        driver._speculation_state = driver._discover_speculate_functions()
        spec_state = driver._speculation_state["fetch_case"]
        spec_state.highest_successful_id = 10

        # Success for ID 3 (below watermark of 10)
        request = scraper.fetch_case(3).speculative("fetch_case", 3)
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="https://example.com/case/3",
            request=request,
        )

        driver._track_speculation_outcome(request, response)

        # Watermark stays at 10 (not lowered to 3)
        assert spec_state.highest_successful_id == 10
        # Failures reset on any success
        assert spec_state.consecutive_failures == 0


# =============================================================================
# Trailing Period Discovery Tests
# =============================================================================


class TrailingPeriodScraper(BaseScraper[dict]):
    """Scraper with YearlySpeculation and no backfill, for testing trailing period."""

    @entry(
        dict,
        speculative=YearlySpeculation(
            backfill=(),
            trailing_period=timedelta(days=60),
            largest_observed_gap=3,
        ),
    )
    def fetch_docket(self, year: int, number: int) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/docket/{year}/{number}",
            ),
            continuation="parse_docket",
            is_speculative=True,
        )

    @step
    def parse_docket(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"ok": True})

    @entry(dict)
    def get_entry(self) -> Generator[Request, None, None]:
        return
        yield


class TestTrailingPeriodDiscovery:
    """Test trailing period auto-creates previous year partition."""

    @freeze_time("2026-01-15")
    def test_previous_year_created_within_trailing_period(self):
        """Previous year partition auto-created when within trailing_period of Jan 1."""
        scraper = TrailingPeriodScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        assert "fetch_docket:2026" in state
        assert "fetch_docket:2025" in state
        assert state["fetch_docket:2025"].frozen is False

    @freeze_time("2026-06-15")
    def test_previous_year_not_created_outside_trailing_period(self):
        """Previous year partition NOT created when well past trailing_period."""
        scraper = TrailingPeriodScraper()
        driver = SyncDriver(scraper)

        state = driver._discover_speculate_functions()

        assert "fetch_docket:2026" in state
        assert "fetch_docket:2025" not in state


# =============================================================================
# Discovery with initial_seed() Overrides
# =============================================================================


class TestDiscoveryWithOverrides:
    """Test that _discover_speculate_functions uses initial_seed() overrides."""

    def test_override_partitions_replace_decorator_backfill(self):
        """Override partitions from initial_seed() replace decorator-defined backfill."""
        scraper = YearlySpeculationTestScraper()

        # Simulate what initial_seed() does: store overrides
        scraper._speculation_overrides = {
            "fetch_docket": [
                {"year": 2025, "number": (1, 50), "frozen": False},
                {"year": 2020, "number": (1, 200), "frozen": True},
            ]
        }

        driver = SyncDriver(scraper)
        state = driver._discover_speculate_functions()

        # Override partitions should be present
        assert "fetch_docket:2025" in state
        assert state["fetch_docket:2025"].config.definite_range == (1, 50)
        assert state["fetch_docket:2025"].frozen is False

        assert "fetch_docket:2020" in state
        assert state["fetch_docket:2020"].config.definite_range == (1, 200)
        assert state["fetch_docket:2020"].frozen is True

        # Decorator-defined backfill (2023, 2024) should NOT be present
        # since overrides replace them entirely
        assert "fetch_docket:2023" not in state
        assert "fetch_docket:2024" not in state


# =============================================================================
# YearlySpeculation End-to-End with HTTP Mocking
# =============================================================================


class TestYearlySpeculationEndToEnd:
    """End-to-end test for YearlySpeculation with mocked HTTP."""

    def test_yearly_end_to_end_processes_partitions(self):
        """Full driver.run() with YearlySpeculation processes correct year/number pairs."""
        scraper = YearlySpeculationTestScraper()
        collected_data: list[dict] = []

        def collect(data: dict) -> None:
            collected_data.append(data)

        def mock_request(**kwargs) -> MagicMock:
            url = kwargs["url"]
            parts = url.rstrip("/").split("/")
            year, number = int(parts[-2]), int(parts[-1])
            resp = MagicMock()
            resp.headers = {}
            # 2023 partition: all succeed (frozen, just backfill)
            # 2024 partition: IDs 1-3 succeed, 4+ fail
            if year == 2023 or number <= 3:
                resp.status_code = 200
            else:
                resp.status_code = 404
            resp.content = b"<html>ok</html>"
            resp.text = "<html>ok</html>"
            return resp

        driver = SyncDriver(scraper, on_data=collect)

        # Override state to just the two backfill partitions for determinism
        driver._speculation_state = driver._discover_speculate_functions()
        driver._speculation_state = {
            k: v
            for k, v in driver._speculation_state.items()
            if k in ("fetch_docket:2023", "fetch_docket:2024")
        }

        # Seed and run manually since we've overridden state
        driver.request_queue = []
        driver._seed_speculative_queue()

        # Mock the HTTP client
        driver.request_manager._client = MagicMock()
        driver.request_manager._client.request.side_effect = mock_request

        # Process queue
        import heapq

        while driver.request_queue:
            _priority, _counter, request = heapq.heappop(driver.request_queue)
            try:
                response = driver.resolve_request(request)
            except Exception:
                continue

            if request.is_speculative:
                driver._track_speculation_outcome(request, response)

            continuation_name = (
                request.continuation
                if isinstance(request.continuation, str)
                else request.continuation.__name__
            )
            continuation = scraper.get_continuation(continuation_name)
            gen = continuation(response)
            driver._process_generator(gen, response, request)

        # 2023 frozen: processed IDs 1-5, stopped after seeding
        assert driver._speculation_state["fetch_docket:2023"].stopped is True
        # 2024 non-frozen: processed IDs 1-3, then failures stop it
        assert driver._speculation_state["fetch_docket:2024"].stopped is True

        # Verify correct year/number combos were processed
        processed_pairs = set(scraper.processed)
        for n in range(1, 6):
            assert (2023, n) in processed_pairs
        for n in range(1, 4):
            assert (2024, n) in processed_pairs
