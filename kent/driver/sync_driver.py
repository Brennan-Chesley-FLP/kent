"""Synchronous driver implementation.

This module contains the sync driver that processes scraper generators.
It evolves across the 29 steps of the design documentation.

- Step 1: A simple function that runs a scraper generator and collects results.
- Step 2: A class-based driver that handles NavigatingRequest, fetches pages,
  and calls continuation methods by name.
- Step 3: Tracks current_location and handles NonNavigatingRequest.
- Step 4: Handles ArchiveRequest to download and save files locally.
- Step 5: No driver changes - accumulated_data flows through requests automatically.
- Step 6: No driver changes - aux_data flows through requests automatically.
- Step 7: Adds on_data callback for side effects (persistence, logging) when data yielded.
- Step 9: Adds on_invalid_data callback for handling validation failures.
- Step 10: Adds on_transient_exception callback for handling transient errors.
- Step 13: Adds on_archive callback for customizing file archival behavior.
- Step 14: Adds on_run_start and on_run_complete lifecycle hooks for tracking scraper runs.
- Step 15: Replaces list queue with heapq priority queue for memory optimization.
- Step 16: Adds deduplication_key field to requests and duplicate_check callback for preventing duplicate requests.
"""

from __future__ import annotations

import heapq
import logging
import threading
from collections.abc import Callable, Generator
from dataclasses import dataclass
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Generic, TypeVar
from urllib.parse import urlparse

from typing_extensions import assert_never

from kent.common.decorators import (
    SpeculateMetadata,
    _get_speculative_axis,
    get_entry_metadata,
)
from kent.common.deferred_validation import (
    DeferredValidation,
)
from kent.common.exceptions import (
    DataFormatAssumptionException,
    ScraperAssumptionException,
    TransientException,
)
from kent.common.request_manager import (
    SyncRequestManager,
)
from kent.common.searchable import (
    SpeculateFunctionConfig,
)
from kent.common.speculation_types import (
    SimpleSpeculation,
    SpeculationType,
    YearlySpeculation,
)
from kent.data_types import (
    ArchiveRequest,
    ArchiveResponse,
    BaseRequest,
    BaseScraper,
    EstimateData,
    NavigatingRequest,
    NonNavigatingRequest,
    ParsedData,
    Response,
    ScraperYield,
    SkipDeduplicationCheck,
)

# =============================================================================
# Step 2: Class-based Driver with HTTP Support
# =============================================================================
# Step 3: current_location tracking and NonNavigatingRequest support
# Step 4: ArchiveRequest handling for file downloads
# Step 9: Data validation with on_invalid_data callback


logger = logging.getLogger(__name__)

ScraperReturnDatatype = TypeVar("ScraperReturnDatatype")


@dataclass
class SpeculationState:
    """Tracks speculation state for a single speculative partition.

    For SimpleSpeculation, there is one SpeculationState per function,
    keyed by func_name.

    For YearlySpeculation, there is one SpeculationState per year partition,
    keyed by ``func_name:year``.

    Attributes:
        func_name: Key for this state. Plain func name for Simple,
            ``func_name:year`` for Yearly.
        speculation: The speculation config from the @entry decorator.
        config: Runtime overrides for definite_range and plus.
        highest_successful_id: Highest ID that returned a successful (2xx) response.
        consecutive_failures: Number of consecutive non-2xx responses beyond highest_successful_id.
        current_ceiling: Highest ID currently seeded to the queue.
        stopped: True when plus consecutive failures reached.
        base_func_name: The actual function name on the scraper (without :year suffix).
        year: For YearlySpeculation partitions, the year. None for Simple.
        frozen: For YearlySpeculation, whether this partition is backfill-only.
    """

    func_name: str
    speculation: SpeculationType
    config: SpeculateFunctionConfig
    highest_successful_id: int = 0
    consecutive_failures: int = 0
    current_ceiling: int = 0
    stopped: bool = False
    base_func_name: str = ""
    year: int | None = None
    frozen: bool = False

    # Legacy field — kept for backward compat with SpeculationMixin
    # that references spec_state.metadata
    metadata: SpeculateMetadata | None = None


def log_and_validate_invalid_data(data: DeferredValidation) -> None:
    """Default callback for invalid data that logs validation errors.

    This callback attempts to validate the data to get detailed error information,
    then logs the validation failure at the error level.

    Args:
        data: DeferredValidation instance containing invalid data.
    """
    try:
        # Attempt validation to get detailed error information
        data.confirm()
    except DataFormatAssumptionException as e:
        # Log the validation failure with full context
        error_summary = ", ".join(
            f"{err['loc'][0]}: {err['msg']}" for err in e.errors
        )
        logger.error(
            f"Data validation failed for model '{e.model_name}': {error_summary}",
            extra={
                "model_name": e.model_name,
                "request_url": e.request_url,
                "error_count": len(e.errors),
                "errors": e.errors,
                "failed_doc": e.failed_doc,
            },
        )


def default_archive_callback(
    content: bytes, url: str, expected_type: str | None, storage_dir: Path
) -> str:
    """Default callback for archiving downloaded files.

    This callback extracts a filename from the URL or generates one based on
    the expected file type, then saves the file to the storage directory.

    Args:
        content: The binary file content.
        url: The URL the file was downloaded from.
        expected_type: Optional hint about the file type.
        storage_dir: Directory where files should be saved.

    Returns:
        The local file path where the file was saved.
    """
    # Extract filename from URL or generate one
    parsed_url = urlparse(url)
    path_parts = Path(parsed_url.path).parts
    # Filter out empty strings, '.', and '/' from path parts
    valid_parts = [p for p in path_parts if p and p not in (".", "/")]

    if valid_parts:
        filename = valid_parts[-1]
    else:
        # Generate a filename based on expected_type
        ext = {"pdf": ".pdf", "audio": ".mp3"}.get(expected_type or "", "")
        filename = f"download_{hash(url)}{ext}"

    file_path = storage_dir / filename
    file_path.write_bytes(content)
    return str(file_path)


class SyncDriver(Generic[ScraperReturnDatatype]):
    """Synchronous driver for running scrapers.

    This Step 4 driver:
    - Maintains a request queue (BaseRequest, not just NavigatingRequest)
    - Fetches URLs using httpx
    - Looks up continuation methods by name
    - Each request carries its own current_location and ancestry
    - Uses exhaustive pattern matching for scraper yields
    - Handles ArchiveRequest to download and save files locally

    Example usage:
        from tests.utils import collect_results

        callback, results = collect_results()
        driver = SyncDriver(scraper, on_data=callback)
        driver.run()
        # Results are now in the results list
    """

    def __init__(
        self,
        scraper: BaseScraper[ScraperReturnDatatype],
        storage_dir: Path | None = None,
        request_manager: SyncRequestManager | None = None,
        on_data: Callable[
            [ScraperReturnDatatype],
            None,
        ]
        | None = None,
        on_structural_error: Callable[[ScraperAssumptionException], bool]
        | None = None,
        on_invalid_data: Callable[[DeferredValidation], None] | None = None,
        on_transient_exception: Callable[[TransientException], bool]
        | None = None,
        on_archive: Callable[[bytes, str, str | None, Path], str]
        | None = None,
        on_run_start: Callable[[str], None] | None = None,
        on_run_complete: Callable[[str, str, Exception | None], None]
        | None = None,
        duplicate_check: Callable[[str], bool] | None = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        """Initialize the driver.

        Args:
            scraper: Scraper instance with continuation methods.
            storage_dir: Directory for storing downloaded files. If None, uses system temp directory.
            request_manager: SyncRequestManager for handling HTTP requests.
            on_data: Optional callback invoked when ParsedData is yielded and validated. Useful for
                persistence, logging, or other side effects. The callback receives the
                unwrapped data from ParsedData.
            on_structural_error: Optional callback invoked when HTMLStructuralAssumptionException
                is raised during scraping. The callback receives the exception and should return
                True to continue scraping or False to stop. If not provided, exceptions propagate
                normally and stop the scraper.
            on_invalid_data: Optional callback invoked when data fails validation. If not provided,
                invalid data is sent to on_data callback (if present), otherwise validation
                exceptions propagate normally.
            on_transient_exception: Optional callback invoked when TransientException is raised
                during HTTP requests. The callback receives the exception and should return True
                to continue scraping or False to stop. If not provided, exceptions propagate
                normally and stop the scraper.
            on_archive: Optional callback invoked when files are archived. Receives content (bytes),
                url (str), expected_type (str | None), and storage_dir (Path). Should return the
                local file path where the file was saved. If not provided, uses default_archive_callback.
            on_run_start: Optional callback invoked when the scraper run starts. Receives scraper_name (str).
            on_run_complete: Optional callback invoked when the scraper run completes. Receives
                scraper_name (str), status ("completed" | "error"),
                and error (Exception | None).
            duplicate_check: Optional callback invoked before enqueuing a request. Receives the
                deduplication_key (str) and should return True to enqueue the request or False to
                skip it. If not provided, all requests are enqueued (no deduplication).
            stop_event: Optional threading.Event for graceful shutdown. When set, the driver
                will stop processing after completing the current request.
        """
        self.scraper = scraper
        # Step 15: Use heapq for priority queue (min heap)
        # Each entry is (priority, counter, request) for stable FIFO ordering
        self.request_queue: list[tuple[int, int, BaseRequest]] = []
        self._queue_counter = 0  # For FIFO tie-breaking within same priority
        # Step 16: Track seen deduplication keys for default duplicate checking
        self._seen_keys: set[str] = set()
        self.storage_dir = (
            storage_dir or Path(gettempdir()) / "juriscraper_files"
        )
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # Set up request manager - either use provided one or create default
        if request_manager is not None:
            self.request_manager = request_manager
            self._owns_request_manager = False
        else:
            self.request_manager = SyncRequestManager(
                ssl_context=scraper.get_ssl_context(),
            )
            self._owns_request_manager = True

        self.on_data = on_data
        self.on_structural_error = on_structural_error
        self.on_invalid_data = on_invalid_data
        self.on_transient_exception = on_transient_exception
        self.on_archive = on_archive or default_archive_callback
        self.on_run_start = on_run_start
        self.on_run_complete = on_run_complete
        self.duplicate_check = duplicate_check
        self.stop_event = stop_event

        # Speculation state - populated by _discover_speculate_functions
        self._speculation_state: dict[str, SpeculationState] = {}

    def _discover_speculate_functions(self) -> dict[str, SpeculationState]:
        """Discover speculative functions on the scraper and initialize tracking state.

        Uses BaseScraper.list_speculative_entries() to find speculative entries.

        For SimpleSpeculation: creates one SpeculationState keyed by func_name.
        For YearlySpeculation: creates one SpeculationState per year partition,
            keyed by ``func_name:year``. Also handles rollover (auto-creating
            current year) and trailing period.

        Returns:
            Dictionary mapping state keys to their SpeculationState.
        """
        from datetime import date as date_cls, timedelta

        state: dict[str, SpeculationState] = {}

        # Check for overrides from initial_seed()
        overrides = getattr(self.scraper, "_speculation_overrides", {})

        for entry_info in self.scraper.list_speculative_entries():
            spec = entry_info.speculation

            if isinstance(spec, SimpleSpeculation):
                metadata = SpeculateMetadata(
                    observation_date=spec.observation_date,
                    highest_observed=spec.highest_observed,
                    largest_observed_gap=spec.largest_observed_gap,
                )
                state[entry_info.name] = SpeculationState(
                    func_name=entry_info.name,
                    speculation=spec,
                    config=SpeculateFunctionConfig(),
                    base_func_name=entry_info.name,
                    metadata=metadata,
                )

            elif isinstance(spec, YearlySpeculation):
                # Determine which partitions to seed
                if entry_info.name in overrides:
                    # Use override partitions from initial_seed()
                    partitions = overrides[entry_info.name]
                else:
                    # Use backfill partitions from decorator
                    partitions = [
                        {
                            "year": p.year,
                            _get_speculative_axis(entry_info.param_types): p.number,
                            "frozen": p.frozen,
                        }
                        for p in spec.backfill
                    ]

                # Create a SpeculationState per year partition
                for partition in partitions:
                    year = partition["year"]
                    axis_name = _get_speculative_axis(entry_info.param_types)
                    number_range = partition[axis_name]
                    frozen = partition.get("frozen", False)
                    key = f"{entry_info.name}:{year}"

                    metadata = SpeculateMetadata(
                        highest_observed=number_range[1],
                        largest_observed_gap=spec.largest_observed_gap,
                    )
                    state[key] = SpeculationState(
                        func_name=key,
                        speculation=spec,
                        config=SpeculateFunctionConfig(
                            definite_range=tuple(number_range),
                        ),
                        base_func_name=entry_info.name,
                        year=year,
                        frozen=frozen,
                        metadata=metadata,
                    )

                # Rollover: auto-create current year if missing
                today = date_cls.today()
                current_year = today.year
                current_key = f"{entry_info.name}:{current_year}"
                if current_key not in state:
                    metadata = SpeculateMetadata(
                        highest_observed=spec.largest_observed_gap,
                        largest_observed_gap=spec.largest_observed_gap,
                    )
                    state[current_key] = SpeculationState(
                        func_name=current_key,
                        speculation=spec,
                        config=SpeculateFunctionConfig(
                            definite_range=(1, spec.largest_observed_gap),
                        ),
                        base_func_name=entry_info.name,
                        year=current_year,
                        frozen=False,
                        metadata=metadata,
                    )

                # Trailing period: ensure previous year is active
                prev_year = current_year - 1
                prev_key = f"{entry_info.name}:{prev_year}"
                jan1 = date_cls(current_year, 1, 1)
                within_trailing = (
                    today - jan1
                ) < spec.trailing_period
                if within_trailing and prev_key not in state:
                    metadata = SpeculateMetadata(
                        highest_observed=spec.largest_observed_gap,
                        largest_observed_gap=spec.largest_observed_gap,
                    )
                    state[prev_key] = SpeculationState(
                        func_name=prev_key,
                        speculation=spec,
                        config=SpeculateFunctionConfig(
                            definite_range=(1, spec.largest_observed_gap),
                        ),
                        base_func_name=entry_info.name,
                        year=prev_year,
                        frozen=False,
                        metadata=metadata,
                    )

        return state

    def _seed_speculative_queue(self) -> None:
        """Seed the queue with initial speculative requests.

        For SimpleSpeculation: calls func(id_value) for each ID in range.
        For YearlySpeculation: calls func(year, number) for each ID in range.

        Uses config.definite_range if set, otherwise falls back to
        (1, highest_observed) from the speculation metadata.
        """
        for state_key, spec_state in self._speculation_state.items():
            # Get the actual function on the scraper
            func = getattr(self.scraper, spec_state.base_func_name)

            # Determine the range
            if spec_state.config.definite_range is not None:
                start, end = spec_state.config.definite_range
            elif spec_state.metadata is not None:
                start = 1
                end = spec_state.metadata.highest_observed
            else:
                continue

            # Seed the queue
            for id_value in range(start, end + 1):
                if spec_state.year is not None:
                    # YearlySpeculation: pass year and number
                    request = func(spec_state.year, id_value)
                else:
                    # SimpleSpeculation: pass just the ID
                    request = func(id_value)
                # Mark as speculative with the state key
                request = request.speculative(state_key, id_value)
                heapq.heappush(
                    self.request_queue,
                    (request.priority, self._queue_counter, request),
                )
                self._queue_counter += 1

            # Update current_ceiling to the highest seeded ID
            spec_state.current_ceiling = end

            # Frozen partitions stop after seeding
            if spec_state.frozen:
                spec_state.stopped = True

    def _extend_speculation(self, state_key: str) -> None:
        """Extend speculation for a partition when approaching the ceiling.

        Called when a speculative request succeeds. If highest_successful_id
        approaches current_ceiling and we haven't hit plus consecutive failures,
        seed additional IDs.

        Frozen partitions never extend.

        Args:
            state_key: Key in _speculation_state (func_name or func_name:year).
        """
        spec_state = self._speculation_state.get(state_key)
        if spec_state is None or spec_state.stopped or spec_state.frozen:
            return

        # Determine plus threshold
        if spec_state.config.plus is not None:
            plus = spec_state.config.plus
        elif isinstance(spec_state.speculation, SimpleSpeculation):
            plus = spec_state.speculation.largest_observed_gap
        elif isinstance(spec_state.speculation, YearlySpeculation):
            plus = spec_state.speculation.largest_observed_gap
        else:
            return

        # If consecutive failures >= plus, stop extending
        if spec_state.consecutive_failures >= plus:
            spec_state.stopped = True
            return

        # Extend if highest_successful_id is near the ceiling
        if (
            spec_state.highest_successful_id
            >= spec_state.current_ceiling - plus
        ):
            func = getattr(self.scraper, spec_state.base_func_name)

            new_ceiling = spec_state.current_ceiling + plus
            for id_value in range(
                spec_state.current_ceiling + 1, new_ceiling + 1
            ):
                if spec_state.year is not None:
                    request = func(spec_state.year, id_value)
                else:
                    request = func(id_value)
                request = request.speculative(state_key, id_value)
                heapq.heappush(
                    self.request_queue,
                    (request.priority, self._queue_counter, request),
                )
                self._queue_counter += 1

            spec_state.current_ceiling = new_ceiling

    def _track_speculation_outcome(
        self, request: BaseRequest, response: Response
    ) -> None:
        """Track the outcome of a speculative request.

        Updates highest_successful_id and consecutive_failures based on response.

        Args:
            request: The speculative request.
            response: The HTTP response.
        """
        if not request.is_speculative or request.speculation_id is None:
            return

        # Extract state key and ID from speculation_id tuple
        state_key, speculative_id = request.speculation_id

        # Find the spec_state for this partition
        spec_state = self._speculation_state.get(state_key)
        if spec_state is None:
            return

        is_success = 200 <= response.status_code < 300
        if is_success and not self.scraper.fails_successfully(response):
            # Soft 404 - treat as failure
            is_success = False

        if is_success:
            # Success - update highest_successful_id and reset failures
            if speculative_id > spec_state.highest_successful_id:
                spec_state.highest_successful_id = speculative_id
            spec_state.consecutive_failures = 0
            # Extend speculation if needed
            self._extend_speculation(state_key)
        else:
            # Failure - increment consecutive_failures if beyond highest_successful_id
            if speculative_id > spec_state.highest_successful_id:
                spec_state.consecutive_failures += 1
                # Check if we should stop
                if spec_state.config.plus is not None:
                    plus = spec_state.config.plus
                elif isinstance(spec_state.speculation, SimpleSpeculation):
                    plus = spec_state.speculation.largest_observed_gap
                elif isinstance(spec_state.speculation, YearlySpeculation):
                    plus = spec_state.speculation.largest_observed_gap
                else:
                    return
                if spec_state.consecutive_failures >= plus:
                    spec_state.stopped = True

    def _get_entry_requests(
        self,
    ) -> Generator[NavigatingRequest, None, None]:
        """Get initial entry requests from the scraper.

        Builds default invocations from @entry-decorated methods and
        dispatches them via initial_seed(). Falls back to calling
        get_entry() directly for scrapers without @entry decorators.

        Yields:
            NavigatingRequest instances for queue initialization.
        """
        entries = self.scraper.list_entries()
        if entries:
            # Build default invocation: call each non-speculative @entry
            # with no params (speculative entries are handled separately)
            invocations: list[dict[str, dict[str, Any]]] = []
            for entry_info in entries:
                if not entry_info.speculative and not entry_info.param_types:
                    invocations.append({entry_info.name: {}})
            if invocations:
                yield from self.scraper.initial_seed(invocations)
                return
        # Fall back to get_entry() for scrapers without @entry decorators
        yield from self.scraper.get_entry()

    def run(self) -> None:
        """Run the scraper starting from the scraper's entry point.

        Data is passed to the on_data callback as it is yielded. If you need to
        collect results, use a callback that appends to a list (see
        tests/design/utils.py::collect_results for a helper function).
        """

        # Step 14: Fire on_run_start callback
        scraper_name = self.scraper.__class__.__name__
        if self.on_run_start:
            self.on_run_start(scraper_name)

        status = "completed"
        error: Exception | None = None

        try:
            # Initialize priority queue with entry requests.
            self.request_queue = []
            for entry_request in self._get_entry_requests():
                heapq.heappush(
                    self.request_queue,
                    (
                        entry_request.priority,
                        self._queue_counter,
                        entry_request,
                    ),
                )
                self._queue_counter += 1

            # Discover and seed speculative requests
            self._speculation_state = self._discover_speculate_functions()
            if self._speculation_state:
                self._seed_speculative_queue()

            while self.request_queue:
                # Check for graceful shutdown before processing next request
                if self.stop_event and self.stop_event.is_set():
                    break

                # Step 15: Pop from heap (lowest priority first)
                _priority, _counter, request = heapq.heappop(
                    self.request_queue
                )

                # Use match/case for exhaustive request type handling
                match request:
                    case (
                        NavigatingRequest()
                        | NonNavigatingRequest()
                        | ArchiveRequest()
                    ):
                        # Normal request flow
                        # Step 10: Wrap request resolution to catch transient exceptions
                        try:
                            response: Response = (
                                self.resolve_archive_request(request)
                                if isinstance(request, ArchiveRequest)
                                else self.resolve_request(request)
                            )
                        except TransientException as e:
                            # Step 10: Handle transient errors via callback
                            if self.on_transient_exception:
                                should_continue = self.on_transient_exception(
                                    e
                                )
                                if not should_continue:
                                    return
                                continue
                            else:
                                raise

                        # Track speculation outcome if this is a speculative request
                        if request.is_speculative:
                            self._track_speculation_outcome(request, response)

                        # Handle Callable continuations (convert to string)
                        continuation_name = (
                            request.continuation
                            if isinstance(request.continuation, str)
                            else request.continuation.__name__
                        )

                        continuation_method = self.scraper.get_continuation(
                            continuation_name
                        )

                        # Process the generator
                        gen = continuation_method(response)
                        self._process_generator(gen, response, request)

                    case _:
                        # Exhaustive match - should never reach here
                        assert_never(request)  # type: ignore[arg-type]

        except Exception as e:
            # Step 14: Capture error for on_run_complete
            status = "error"
            error = e
            raise
        finally:
            # Close request manager if we own it
            if self._owns_request_manager:
                self.request_manager.close()

            # Step 14: Fire on_run_complete callback
            if self.on_run_complete:
                self.on_run_complete(
                    scraper_name,
                    status,
                    error,
                )

    def enqueue_request(
        self, new_request: BaseRequest, context: Response | BaseRequest
    ) -> None:
        """Enqueue a new request, resolving it from the given context.

        Step 16: Check for duplicates using duplicate_check callback before enqueuing.

        For NavigatingRequest yields: context is the Response
        For NonNavigatingRequest yields: context is the originating request
        For ArchiveRequest yields: context is the Response

        Args:
            new_request: The new request to enqueue.
            context: Response or originating request for URL resolution.
        """
        # Use the request's resolve_from method with the appropriate context
        resolved_request = new_request.resolve_from(context)  # type: ignore

        # Step 16: Check for duplicates before enqueuing
        dedup_key = resolved_request.deduplication_key

        match dedup_key:
            case None:
                pass
            case SkipDeduplicationCheck():
                pass
            case str():
                if self.duplicate_check and not self.duplicate_check(
                    dedup_key
                ):
                    return

        # Step 15: Push onto heap with priority and counter for stable ordering
        heapq.heappush(
            self.request_queue,
            (resolved_request.priority, self._queue_counter, resolved_request),
        )
        self._queue_counter += 1

    def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest and return the Response.

        Delegates to the request manager for HTTP handling.

        Args:
            request: The BaseRequest to fetch.

        Returns:
            Response containing the HTTP response data.

        Raises:
            HTMLResponseAssumptionException: If server returns 5xx status code.
            httpx.TimeoutException: If request times out (for retry handling).
        """
        return self.request_manager.resolve_request(request)

    def resolve_archive_request(
        self, request: ArchiveRequest
    ) -> ArchiveResponse:
        """Fetch an ArchiveRequest, download the file, and return an ArchiveResponse.

        This method fetches the file, calls the on_archive callback to save it
        to local storage, and returns an ArchiveResponse with the file_url field
        populated.

        Args:
            request: The ArchiveRequest to fetch.

        Returns:
            ArchiveResponse containing the HTTP response data and local file path.
        """
        http_response = self.resolve_request(request)

        # Step 13: Use on_archive callback to save the file
        file_url = self.on_archive(
            http_response.content,
            request.request.url,
            request.expected_type,
            self.storage_dir,
        )

        return ArchiveResponse(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=request.request.url,
            request=request,
            file_url=file_url,
        )

    def handle_data(self, data: ScraperReturnDatatype) -> None:
        # Step 9: Validate deferred data if present
        if isinstance(data, DeferredValidation):
            try:
                validated_data: ScraperReturnDatatype = (
                    data.confirm()
                )  # ty: ignore[invalid-assignment]
                # Validation succeeded - send to on_data callback
                if self.on_data:
                    self.on_data(validated_data)
            except DataFormatAssumptionException:
                # Validation failed - use callback hierarchy
                if self.on_invalid_data:
                    self.on_invalid_data(data)
                else:
                    # No callbacks - re-raise the exception
                    raise
        else:
            # Step 7: Not deferred validation - invoke callback if provided
            if self.on_data:
                self.on_data(data)

    def _process_generator(
        self,
        gen: Generator[ScraperYield, bool | None, None],
        response: Response,
        parent_request: BaseRequest,
    ) -> None:
        """Process generator yields, enqueueing requests and handling data.

        Args:
            gen: The generator from the continuation method.
            response: The Response that triggered this continuation.
            parent_request: The request that initiated this continuation.
        """
        try:
            for item in gen:
                match item:
                    case ParsedData():
                        self.handle_data(item.unwrap())
                    case EstimateData():
                        pass
                    case NavigatingRequest():
                        self.enqueue_request(item, response)
                    case NonNavigatingRequest() | ArchiveRequest():
                        self.enqueue_request(item, parent_request)
                    case None:
                        pass
                    case _:
                        assert_never(item)
        except ScraperAssumptionException as e:
            # Step 8: Handle structural errors via callback
            if self.on_structural_error:
                should_continue = self.on_structural_error(e)
                if not should_continue:
                    return
            else:
                raise
