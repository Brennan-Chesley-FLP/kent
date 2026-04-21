"""Tests for the Speculative protocol and driver speculation behavior.

This test module verifies:
- Speculative protocol detection via @entry decorator
- SyncDriver discovery, seeding, tracking, extension, and stopping
- check_success() split between speculative and non-speculative requests
- max_gap() == 0 for frozen ranges
- Multiple templates for the same entry (param_index)
- fails_successfully() soft-404 detection
- End-to-end speculation with mock request manager
"""

from collections.abc import Generator
from unittest.mock import MagicMock

from pydantic import BaseModel

from kent.common.decorators import (
    entry,
    get_entry_metadata,
    step,
)
from kent.common.speculative import Speculative
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

# ── Test Speculative models ────────────────────────────────────────


class CaseId(BaseModel):
    """Simple speculative parameter model for testing."""

    case_id: int
    soft_max: int = 0
    should_advance: bool = True
    gap: int = 2

    def seed_range(self) -> range:
        return range(self.case_id, self.soft_max)

    def from_int(self, n: int) -> "CaseId":
        return CaseId(
            case_id=n,
            soft_max=self.soft_max,
            should_advance=self.should_advance,
            gap=self.gap,
        )

    def max_gap(self) -> int:
        return self.gap


class DocketId(BaseModel):
    """Year-partitioned speculative parameter model for testing."""

    year: int
    number: int
    soft_max: int = 0
    should_advance: bool = True
    gap: int = 2

    def seed_range(self) -> range:
        return range(self.number, self.soft_max)

    def from_int(self, n: int) -> "DocketId":
        return DocketId(
            year=self.year,
            number=n,
            soft_max=self.soft_max,
            should_advance=self.should_advance,
            gap=self.gap,
        )

    def max_gap(self) -> int:
        return self.gap


# ── Test scrapers ──────────────────────────────────────────────────


class SpeculationTestScraper(BaseScraper[dict]):
    """Simple scraper with a speculative entry for unit tests."""

    def __init__(self) -> None:
        super().__init__()
        self.processed_ids: list[int] = []

    @entry(dict)
    def fetch_case(self, cid: CaseId) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/case/{cid.case_id}",
            ),
            continuation="parse_case",
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
        yield  # Make this a generator


# ── Protocol detection tests ───────────────────────────────────────


class TestSpeculativeProtocol:
    # Note: ``issubclass(X, Speculative)`` no longer works because the
    # Protocol has a non-method attribute (``should_advance``). The
    # ``@entry`` decorator uses a structural helper instead; user-facing
    # code relies on ``isinstance(instance, Speculative)``.

    def test_isinstance_with_pydantic_instance(self):
        assert isinstance(CaseId(case_id=1), Speculative)
        assert isinstance(DocketId(year=2024, number=1), Speculative)

    def test_non_speculative_model_not_detected(self):
        class PlainModel(BaseModel):
            name: str

        assert not isinstance(PlainModel(name="x"), Speculative)

    def test_entry_auto_detects_speculative_param(self):
        meta = get_entry_metadata(SpeculationTestScraper.fetch_case)
        assert meta is not None
        assert meta.speculative is True
        assert meta.speculative_param == "cid"

    def test_entry_detects_non_speculative(self):
        meta = get_entry_metadata(SpeculationTestScraper.get_entry)
        assert meta is not None
        assert meta.speculative is False
        assert meta.speculative_param is None


# ── Request.speculative() ──────────────────────────────────────────


class TestIsSpeculativeField:
    def test_is_speculative_defaults_to_false(self):
        request = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="step",
        )
        assert request.is_speculative is False
        assert request.speculation_id is None

    def test_speculative_method_sets_3_tuple(self):
        request = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="step",
        )
        spec = request.speculative("fetch_case", 0, 42)
        assert spec.is_speculative is True
        assert spec.speculation_id == ("fetch_case", 0, 42)

    def test_speculative_preserves_fields(self):
        request = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="step",
            accumulated_data={"key": "value"},
        )
        spec = request.speculative("f", 1, 99)
        assert spec.request.url == "/test"
        assert spec.continuation == "step"
        assert spec.accumulated_data == {"key": "value"}


# ── SyncDriver discovery and seeding ──────────────────────────────


class TestSyncDriverSpeculationDiscovery:
    def test_discovers_from_templates(self):
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": 5, "gap": 2}}}]
            )
        )
        driver = SyncDriver(scraper)
        state = driver._discover_speculate_functions()

        assert "fetch_case:0" in state
        assert state["fetch_case:0"].template.case_id == 5
        assert state["fetch_case:0"].param_index == 0

    def test_no_templates_no_state(self):
        scraper = SpeculationTestScraper()
        driver = SyncDriver(scraper)
        state = driver._discover_speculate_functions()
        assert state == {}

    def test_multiple_templates_same_entry(self):
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {"fetch_case": {"cid": {"case_id": 10, "gap": 3}}},
                    {"fetch_case": {"cid": {"case_id": 20, "gap": 5}}},
                ]
            )
        )
        driver = SyncDriver(scraper)
        state = driver._discover_speculate_functions()

        assert "fetch_case:0" in state
        assert "fetch_case:1" in state
        assert state["fetch_case:0"].template.case_id == 10
        assert state["fetch_case:1"].template.case_id == 20
        assert state["fetch_case:0"].param_index == 0
        assert state["fetch_case:1"].param_index == 1


class TestSyncDriverSpeculationSeeding:
    def test_seeds_speculative_window_from_to_int(self):
        """Seeding starts at to_int() and seeds max_gap() speculative requests."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": 5, "gap": 3}}}]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        # seed_range(5, 0) is empty; advance window of 3 starts at 5 → [5,6,7].
        assert len(driver.request_queue) == 3
        urls = {req.request.url for _p, _c, req in driver.request_queue}
        expected = {f"https://example.com/case/{i}" for i in range(5, 8)}
        assert urls == expected

    def test_empty_seed_range_only_advance_window(self):
        """Empty seed_range + should_advance=True seeds only the window."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": 3, "gap": 2}}}]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        # seed_range(3, 0) is empty; window of 2 starts at window_start = 3.
        assert len(driver.request_queue) == 2
        for _p, _c, req in driver.request_queue:
            assert req.is_speculative is True

    def test_seed_range_plus_window_all_speculative(self):
        """seed_range yields speculative IDs and the advance window adds more."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {
                        "fetch_case": {
                            "cid": {"case_id": 1, "gap": 2, "soft_max": 3}
                        }
                    }
                ]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        # seed_range(1, 3) = [1, 2] (2 speculative)
        # Phase 2 window starts at 3, gap=2 → [3, 4] (2 speculative)
        assert len(driver.request_queue) == 4
        for _p, _c, req in driver.request_queue:
            assert req.is_speculative is True

    def test_frozen_seeds_range_then_stops(self):
        """gap=0 seeds seed_range as speculative, then stops without advancing."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {
                        "fetch_case": {
                            "cid": {"case_id": 1, "soft_max": 5, "gap": 0}
                        }
                    }
                ]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        state = driver._speculation_state["fetch_case:0"]
        assert state.stopped is True
        # seed_range(1, 5) = [1, 2, 3, 4]; window skipped (gap=0).
        assert len(driver.request_queue) == 4
        for _p, _c, req in driver.request_queue:
            assert req.is_speculative is True

    def test_empty_seed_range_and_no_advance_seeds_nothing(self):
        """Empty seed_range with gap=0 seeds nothing."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {
                        "fetch_case": {
                            "cid": {"case_id": 5, "soft_max": 3, "gap": 0}
                        }
                    }
                ]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        state = driver._speculation_state["fetch_case:0"]
        assert state.stopped is True
        assert len(driver.request_queue) == 0

    def test_should_advance_false_seeds_range_only(self):
        """should_advance=False seeds seed_range only, no advance window."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {
                        "fetch_case": {
                            "cid": {
                                "case_id": 1,
                                "soft_max": 4,
                                "gap": 5,
                                "should_advance": False,
                            }
                        }
                    }
                ]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        driver._seed_speculative_queue()

        state = driver._speculation_state["fetch_case:0"]
        assert state.stopped is True
        # seed_range(1, 4) = [1, 2, 3]; advance window skipped.
        assert len(driver.request_queue) == 3
        for _p, _c, req in driver.request_queue:
            assert req.is_speculative is True


# ── Tracking ───────────────────────────────────────────────────────


class TestSyncDriverSpeculationTracking:
    def _make_driver(self, case_id=5, gap=2, soft_max=0):
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [
                    {
                        "fetch_case": {
                            "cid": {
                                "case_id": case_id,
                                "gap": gap,
                                "soft_max": soft_max,
                            }
                        }
                    }
                ]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        return driver

    def _make_response(self, url, status_code=200, request=None):
        return Response(
            status_code=status_code,
            headers={},
            content=b"",
            text="",
            url=url,
            request=request
            or Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url=url),
                continuation="parse_case",
            ),
        )

    def test_success_updates_highest(self):
        driver = self._make_driver()
        state = driver._speculation_state["fetch_case:0"]

        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/3",
            ),
            continuation="parse_case",
            is_speculative=True,
            speculation_id=("fetch_case:0", 0, 3),
        )
        response = self._make_response(
            "https://example.com/case/3", request=request
        )
        driver._track_speculation_outcome(request, response)

        assert state.highest_successful_id == 3
        assert state.consecutive_failures == 0

    def test_failure_increments_consecutive(self):
        driver = self._make_driver()
        state = driver._speculation_state["fetch_case:0"]
        state.highest_successful_id = 2

        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/4",
            ),
            continuation="parse_case",
            is_speculative=True,
            speculation_id=("fetch_case:0", 0, 4),
        )
        response = self._make_response(
            "https://example.com/case/4", status_code=404, request=request
        )
        driver._track_speculation_outcome(request, response)

        assert state.highest_successful_id == 2
        assert state.consecutive_failures == 1

    def test_stops_after_max_gap_failures(self):
        driver = self._make_driver(gap=2)
        state = driver._speculation_state["fetch_case:0"]
        state.highest_successful_id = 2
        state.consecutive_failures = 1  # Already 1 failure

        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/4",
            ),
            continuation="parse_case",
            is_speculative=True,
            speculation_id=("fetch_case:0", 0, 4),
        )
        response = self._make_response(
            "https://example.com/case/4", status_code=404, request=request
        )
        driver._track_speculation_outcome(request, response)

        assert state.consecutive_failures == 2
        assert state.stopped is True

    def test_failure_below_watermark_ignored(self):
        driver = self._make_driver()
        state = driver._speculation_state["fetch_case:0"]
        state.highest_successful_id = 10

        # ID 5 is below highest (10) — failure should be ignored
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/5",
            ),
            continuation="parse_case",
            is_speculative=True,
            speculation_id=("fetch_case:0", 0, 5),
        )
        response = self._make_response(
            "https://example.com/case/5", status_code=404, request=request
        )
        driver._track_speculation_outcome(request, response)

        assert state.consecutive_failures == 0

    def test_non_speculative_request_ignored(self):
        driver = self._make_driver()
        state = driver._speculation_state["fetch_case:0"]

        request = Request(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
            continuation="parse_case",
            is_speculative=False,
        )
        response = self._make_response("/test", request=request)
        driver._track_speculation_outcome(request, response)

        assert state.highest_successful_id == 0


# ── Extension ──────────────────────────────────────────────────────


class TestExtendSpeculation:
    def _make_driver(self, case_id=5, gap=3):
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": case_id, "gap": gap}}}]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        return driver

    def test_extends_when_near_ceiling(self):
        driver = self._make_driver(case_id=5, gap=3)
        state = driver._speculation_state["fetch_case:0"]
        state.current_ceiling = 5
        state.highest_successful_id = 3  # Near ceiling (5 - 3 = 2, 3 >= 2)

        driver._extend_speculation("fetch_case:0")

        assert state.current_ceiling == 8  # Extended by gap=3
        assert len(driver.request_queue) == 3  # 3 new requests (6, 7, 8)

    def test_does_not_extend_when_far_from_ceiling(self):
        driver = self._make_driver(case_id=10, gap=3)
        state = driver._speculation_state["fetch_case:0"]
        state.current_ceiling = 10
        state.highest_successful_id = 2  # Far from ceiling

        driver._extend_speculation("fetch_case:0")

        assert state.current_ceiling == 10  # No change
        assert len(driver.request_queue) == 0

    def test_frozen_not_extended(self):
        """max_gap() == 0 means never extend."""
        scraper = SpeculationTestScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": 5, "gap": 0}}}]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        state = driver._speculation_state["fetch_case:0"]
        state.current_ceiling = 5
        state.highest_successful_id = 5

        driver._extend_speculation("fetch_case:0")

        assert state.current_ceiling == 5  # No extension
        assert len(driver.request_queue) == 0

    def test_stopped_not_extended(self):
        driver = self._make_driver(case_id=5, gap=3)
        state = driver._speculation_state["fetch_case:0"]
        state.stopped = True
        state.current_ceiling = 5
        state.highest_successful_id = 5

        driver._extend_speculation("fetch_case:0")

        assert state.current_ceiling == 5
        assert len(driver.request_queue) == 0


# ── Soft 404 / fails_successfully ──────────────────────────────────


class SoftFourOhFourScraper(BaseScraper[dict]):
    def __init__(self) -> None:
        super().__init__()
        self.processed_ids: list[int] = []

    @entry(dict)
    def fetch_case(self, cid: CaseId) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/case/{cid.case_id}",
            ),
            continuation="parse_case",
        )

    @step
    def parse_case(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"id": 1})

    def fails_successfully(self, response: Response) -> bool:
        return "Case Not Found" not in (response.text or "")


class TestFailsSuccessfully:
    def test_default_returns_true(self):
        scraper = BaseScraper()
        response = Response(
            status_code=200,
            headers={},
            content=b"",
            text="",
            url="/test",
            request=Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
                continuation="step",
            ),
        )
        assert scraper.fails_successfully(response) is True

    def test_override_detects_soft_404(self):
        scraper = SoftFourOhFourScraper()

        response = Response(
            status_code=200,
            headers={},
            content=b"Case Not Found",
            text="Case Not Found",
            url="/test",
            request=Request(
                request=HTTPRequestParams(method=HttpMethod.GET, url="/test"),
                continuation="step",
            ),
        )
        assert scraper.fails_successfully(response) is False

    def test_soft_404_treated_as_failure_in_tracking(self):
        scraper = SoftFourOhFourScraper()
        list(
            scraper.initial_seed(
                [{"fetch_case": {"cid": {"case_id": 5, "gap": 2}}}]
            )
        )
        driver = SyncDriver(scraper)
        driver._speculation_state = driver._discover_speculate_functions()
        state = driver._speculation_state["fetch_case:0"]

        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/case/3",
            ),
            continuation="parse_case",
            is_speculative=True,
            speculation_id=("fetch_case:0", 0, 3),
        )
        response = Response(
            status_code=200,
            headers={},
            content=b"Case Not Found",
            text="Case Not Found",
            url="https://example.com/case/3",
            request=request,
        )
        driver._track_speculation_outcome(request, response)

        # Soft 404 should count as failure
        assert state.highest_successful_id == 0
        assert state.consecutive_failures == 1


# ── End-to-end with mock request manager ───────────────────────────


class EndToEndScraper(BaseScraper[dict]):
    def __init__(self) -> None:
        super().__init__()
        self.results: list[int] = []

    @entry(dict)
    def fetch_case(self, cid: CaseId) -> Request:
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"https://example.com/case/{cid.case_id}",
            ),
            continuation="parse_case",
        )

    @step
    def parse_case(
        self, response: Response
    ) -> Generator[ScraperYield, None, None]:
        case_id = int(response.url.split("/")[-1])
        self.results.append(case_id)
        yield ParsedData({"case_id": case_id})


class TestSyncDriverEndToEnd:
    def test_stops_after_consecutive_failures(self):
        """End-to-end: driver stops extending after max_gap consecutive 404s."""
        scraper = EndToEndScraper()
        # case_id=1 (floor), gap=2, default soft_max=0 → seed_range empty;
        # advance window [1, 2] enqueued speculative.
        seed = [{"fetch_case": {"cid": {"case_id": 1, "gap": 2}}}]

        # IDs 1-3 succeed, 4+ fail
        def resolve(request):
            case_id = int(request.request.url.split("/")[-1])
            status = 200 if case_id <= 3 else 404
            return Response(
                status_code=status,
                headers={},
                content=b"ok" if status == 200 else b"not found",
                text="ok" if status == 200 else "not found",
                url=request.request.url,
                request=request,
            )

        mock_manager = MagicMock()
        mock_manager.resolve_request.side_effect = resolve
        mock_manager.close = MagicMock()

        callback, results = _collect()
        driver = SyncDriver(
            scraper, on_data=callback, request_manager=mock_manager
        )
        driver.seed_params = seed
        driver.run()

        # Initial: 1,2 speculative. 1,2 succeed → extend to 3,4.
        # 3 succeeds → extend to 5,6. 4,5,6 fail → 2 consecutive → stopped.
        # All seeded IDs get processed.
        assert 1 in scraper.results
        assert 2 in scraper.results
        assert 3 in scraper.results
        state = driver._speculation_state["fetch_case:0"]
        assert state.stopped is True

    def test_resets_failure_count_on_success(self):
        """Interleaved successes reset the failure counter."""
        scraper = EndToEndScraper()
        # case_id=1, gap=3, default soft_max=0 → seed_range empty;
        # advance window [1, 2, 3] enqueued speculative.
        seed = [{"fetch_case": {"cid": {"case_id": 1, "gap": 3}}}]

        # IDs 1,2,4,5,7,8 succeed; 3,6 fail (but never 3 consecutive)
        success_ids = {1, 2, 4, 5, 7, 8}

        def resolve(request):
            case_id = int(request.request.url.split("/")[-1])
            status = 200 if case_id in success_ids else 404
            return Response(
                status_code=status,
                headers={},
                content=b"ok" if status == 200 else b"not found",
                text="ok" if status == 200 else "not found",
                url=request.request.url,
                request=request,
            )

        mock_manager = MagicMock()
        mock_manager.resolve_request.side_effect = resolve
        mock_manager.close = MagicMock()

        callback, results = _collect()
        driver = SyncDriver(
            scraper, on_data=callback, request_manager=mock_manager
        )
        driver.seed_params = seed
        driver.run()

        # All 8 initial IDs are processed. Interleaved successes
        # reset the failure counter so speculation extends past 8.
        assert set(range(1, 9)).issubset(set(scraper.results))


def _collect():
    """Helper to collect results from on_data callback."""
    results = []

    def callback(data):
        results.append(data)

    return callback, results
