"""Asynchronous driver implementation.

This module contains the async driver that processes scraper generators
using multiple concurrent workers.

The AsyncDriver closely mirrors SyncDriver with three key differences:

1. Factors out the main run loop to a worker method for concurrency
2. Uses an async-compatible priority queue (asyncio.PriorityQueue)
3. Takes num_workers argument to control concurrency
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable, Generator
from pathlib import Path
from tempfile import gettempdir
from typing import Any, Generic, TypeVar
from urllib.parse import urlparse

from typing_extensions import assert_never

from kent.common.decorators import (
    SpeculateMetadata,
    get_entry_metadata,
)
from kent.common.deferred_validation import (
    DeferredValidation,
)
from kent.common.exceptions import (
    DataFormatAssumptionException,
    HTMLStructuralAssumptionException,
    RequestFailedHalt,
    RequestFailedSkip,
    TransientException,
)
from kent.common.request_manager import (
    AsyncRequestManager,
)
from kent.common.searchable import (
    SpeculateFunctionConfig,
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
from kent.driver.sync_driver import SpeculationState

logger = logging.getLogger(__name__)

ScraperReturnDatatype = TypeVar("ScraperReturnDatatype")


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


async def default_archive_callback(
    content: bytes, url: str, expected_type: str | None, storage_dir: Path
) -> str:
    """Default async callback for archiving downloaded files.

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


class AsyncDriver(Generic[ScraperReturnDatatype]):
    """Asynchronous driver for running scrapers with multiple workers.

    This driver closely mirrors SyncDriver with three key differences:
    - Uses asyncio.PriorityQueue for async-compatible priority queue
    - Factors out the main loop to _worker() for concurrent execution
    - Takes num_workers to control the number of concurrent workers

    Example usage:
        from tests.utils import collect_results

        callback, results = collect_results()
        driver = AsyncDriver(scraper, on_data=callback, num_workers=4)
        await driver.run()
        # Results are now in the results list
    """

    def __init__(
        self,
        scraper: BaseScraper[ScraperReturnDatatype],
        storage_dir: Path | None = None,
        request_manager: AsyncRequestManager | None = None,
        on_data: Callable[
            [ScraperReturnDatatype],
            Awaitable[None],
        ]
        | None = None,
        on_structural_error: Callable[
            [HTMLStructuralAssumptionException], Awaitable[bool]
        ]
        | None = None,
        on_invalid_data: Callable[[DeferredValidation], Awaitable[None]]
        | None = None,
        on_transient_exception: Callable[[TransientException], Awaitable[bool]]
        | None = None,
        on_archive: Callable[[bytes, str, str | None, Path], Awaitable[str]]
        | None = None,
        on_run_start: Callable[[str], Awaitable[None]] | None = None,
        on_run_complete: Callable[
            [str, str, Exception | None], Awaitable[None]
        ]
        | None = None,
        duplicate_check: Callable[[str], Awaitable[bool]] | None = None,
        stop_event: asyncio.Event | None = None,
        num_workers: int = 1,
    ) -> None:
        """Initialize the driver.

        Args:
            scraper: Scraper instance with continuation methods.
            storage_dir: Directory for storing downloaded files. If None, uses system temp directory.
            request_manager: AsyncRequestManager for handling HTTP requests.
            on_data: Optional async callback invoked when ParsedData is yielded and validated. Useful
                for persistence, logging, or other side effects. The callback receives the
                unwrapped data from ParsedData.
            on_structural_error: Optional async callback invoked when HTMLStructuralAssumptionException
                is raised during scraping. The callback receives the exception and should return
                True to continue scraping or False to stop. If not provided, exceptions propagate
                normally and stop the scraper.
            on_invalid_data: Optional async callback invoked when data fails validation. If not
                provided, invalid data is sent to on_data callback (if present), otherwise validation
                exceptions propagate normally.
            on_transient_exception: Optional async callback invoked when TransientException is raised
                during HTTP requests. The callback receives the exception and should return True
                to continue scraping or False to stop. If not provided, exceptions propagate
                normally and stop the scraper.
            on_archive: Optional async callback invoked when files are archived. Receives content
                (bytes), url (str), expected_type (str | None), and storage_dir (Path). Should return
                the local file path where the file was saved. If not provided, uses default_archive_callback.
            on_run_start: Optional async callback invoked when the scraper run starts. Receives
                scraper_name (str).
            on_run_complete: Optional async callback invoked when the scraper run completes. Receives
                scraper_name (str), status ("completed" | "error")
                and error (Exception | None).
            duplicate_check: Optional async callback invoked before enqueuing a request. Receives the
                deduplication_key (str) and should return True to enqueue the request or False to
                skip it. If not provided, all requests are enqueued (no deduplication).
            stop_event: Optional asyncio.Event for graceful shutdown. When set, workers
                will stop processing after completing their current request.
            num_workers: Number of concurrent workers to process requests. Defaults to 1.
        """
        self.scraper = scraper
        # Use asyncio.PriorityQueue for async-compatible priority queue
        # Each entry is (priority, counter, request) for stable FIFO ordering
        self.request_queue: asyncio.PriorityQueue[
            tuple[int, int, BaseRequest]
        ] = asyncio.PriorityQueue()
        self._queue_counter = 0  # For FIFO tie-breaking within same priority
        self._queue_lock = asyncio.Lock()  # Protect counter increments
        self.storage_dir = (
            storage_dir or Path(gettempdir()) / "juriscraper_files"
        )
        self.storage_dir.mkdir(parents=True, exist_ok=True)

        # Set up request manager - either use provided one or create default
        if request_manager is not None:
            self.request_manager = request_manager
            self._owns_request_manager = False
        else:
            self.request_manager = AsyncRequestManager(
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
        self.num_workers = num_workers

        # Speculation state - populated by _discover_speculate_functions
        self._speculation_state: dict[str, SpeculationState] = {}
        # Lock for speculation state updates from concurrent workers
        self._speculation_lock = asyncio.Lock()

    def _discover_speculate_functions(self) -> dict[str, SpeculationState]:
        """Discover speculative functions on the scraper and initialize tracking state.

        Finds methods decorated with @entry(speculative=True) and creates
        SpeculationState for each.

        Returns:
            Dictionary mapping function names to their SpeculationState.
        """
        state: dict[str, SpeculationState] = {}

        for name in dir(self.scraper):
            if name.startswith("_"):
                continue
            func = getattr(self.scraper, name, None)
            if func is None:
                continue

            entry_meta = get_entry_metadata(func)
            if entry_meta is not None and entry_meta.speculative:
                metadata = SpeculateMetadata(
                    observation_date=entry_meta.observation_date,
                    highest_observed=entry_meta.highest_observed,
                    largest_observed_gap=entry_meta.largest_observed_gap,
                )
                state[name] = SpeculationState(
                    func_name=name,
                    metadata=metadata,
                    config=SpeculateFunctionConfig(),
                )

        return state

    async def _seed_speculative_queue(self) -> None:
        """Seed the queue with initial speculative requests based on params config.

        For each @speculate function:
        - If definite_range is configured, use that range
        - Otherwise, use (1, highest_observed) from decorator metadata
        - Enqueue requests for all IDs in the range
        """
        for func_name, spec_state in self._speculation_state.items():
            # Get the speculate function
            func = getattr(self.scraper, func_name)

            # Determine the range
            if spec_state.config.definite_range is not None:
                start, end = spec_state.config.definite_range
            else:
                # Use defaults from decorator metadata
                start = 1
                end = spec_state.metadata.highest_observed

            # Seed the queue
            async with self._queue_lock:
                for id_value in range(start, end + 1):
                    request = func(id_value)
                    # Ensure speculative fields are set
                    request = request.speculative(func_name, id_value)
                    await self.request_queue.put(
                        (request.priority, self._queue_counter, request)
                    )
                    self._queue_counter += 1

            # Update current_ceiling to the highest seeded ID
            spec_state.current_ceiling = end

    async def _extend_speculation(self, func_name: str) -> None:
        """Extend speculation for a function when approaching the ceiling.

        Called when a speculative request succeeds. If highest_successful_id
        approaches current_ceiling and we haven't hit plus consecutive failures,
        seed additional IDs.

        Args:
            func_name: Name of the @speculate function to extend.
        """
        spec_state = self._speculation_state.get(func_name)
        if spec_state is None or spec_state.stopped:
            return

        # Determine plus threshold
        if spec_state.config.plus is not None:
            plus = spec_state.config.plus
        else:
            plus = spec_state.metadata.largest_observed_gap

        # If consecutive failures >= plus, stop extending
        if spec_state.consecutive_failures >= plus:
            spec_state.stopped = True
            return

        # Extend if highest_successful_id is near the ceiling
        # We extend when within 'plus' of the ceiling
        if (
            spec_state.highest_successful_id
            >= spec_state.current_ceiling - plus
        ):
            # Get the speculate function
            func = getattr(self.scraper, func_name)

            # Seed additional IDs up to ceiling + plus
            new_ceiling = spec_state.current_ceiling + plus
            async with self._queue_lock:
                for id_value in range(
                    spec_state.current_ceiling + 1, new_ceiling + 1
                ):
                    request = func(id_value)
                    # Ensure speculative fields are set
                    request = request.speculative(func_name, id_value)
                    await self.request_queue.put(
                        (request.priority, self._queue_counter, request)
                    )
                    self._queue_counter += 1

            spec_state.current_ceiling = new_ceiling

    async def _track_speculation_outcome(
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

        # Extract function name and ID from speculation_id tuple
        func_name, speculative_id = request.speculation_id

        # Find the spec_state for this function
        spec_state = self._speculation_state.get(func_name)
        if spec_state is None:
            return

        is_success = 200 <= response.status_code < 300
        if is_success and not self.scraper.fails_successfully(response):
            # Soft 404 - treat as failure
            is_success = False

        async with self._speculation_lock:
            if is_success:
                # Success - update highest_successful_id and reset failures
                if speculative_id > spec_state.highest_successful_id:
                    spec_state.highest_successful_id = speculative_id
                spec_state.consecutive_failures = 0
                # Extend speculation if needed
                await self._extend_speculation(spec_state.func_name)
            else:
                # Failure - increment consecutive_failures if beyond highest_successful_id
                if speculative_id > spec_state.highest_successful_id:
                    spec_state.consecutive_failures += 1
                    # Check if we should stop
                    plus = (
                        spec_state.config.plus
                        if spec_state.config.plus is not None
                        else spec_state.metadata.largest_observed_gap
                    )
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

    async def run(self) -> None:
        """Run the scraper starting from the scraper's entry point.

        Data is passed to the on_data callback as it is yielded. If you need to
        collect results, use a callback that appends to a list (see
        tests/design/utils.py::collect_results for a helper function).
        """

        # Fire on_run_start callback
        scraper_name = self.scraper.__class__.__name__
        if self.on_run_start:
            await self.on_run_start(scraper_name)

        status = "completed"
        error: Exception | None = None

        try:
            # Check for early stop before doing any work
            if self.stop_event and self.stop_event.is_set():
                return

            # Initialize priority queue with entry requests.
            self.request_queue = asyncio.PriorityQueue()
            self._queue_counter = 0
            for entry_request in self._get_entry_requests():
                await self.request_queue.put(
                    (
                        entry_request.priority,
                        self._queue_counter,
                        entry_request,
                    )
                )
                self._queue_counter += 1

            # Discover and seed speculative requests
            self._speculation_state = self._discover_speculate_functions()
            if self._speculation_state:
                await self._seed_speculative_queue()

            # Start workers
            workers = [
                asyncio.create_task(self._worker(i))
                for i in range(self.num_workers)
            ]

            # Wait for all items in the queue to be processed
            # Use wait_for with periodic checks for stop_event
            while True:
                if self.stop_event and self.stop_event.is_set():
                    # Stop requested - cancel workers and drain queue
                    for worker in workers:
                        worker.cancel()
                    # Drain the queue to prevent join() from blocking
                    while not self.request_queue.empty():
                        try:
                            self.request_queue.get_nowait()
                            self.request_queue.task_done()
                        except asyncio.QueueEmpty:
                            break
                    break

                try:
                    await asyncio.wait_for(
                        asyncio.shield(self.request_queue.join()), timeout=0.1
                    )
                    # join() completed - all work is done
                    break
                except TimeoutError:
                    # Check stop_event and continue waiting
                    continue

            # Cancel workers (they're waiting on the queue)
            for worker in workers:
                worker.cancel()

            # Wait for workers to finish cancellation
            await asyncio.gather(*workers, return_exceptions=True)

        except Exception as e:
            # Capture error for on_run_complete
            status = "error"
            error = e
            raise
        finally:
            # Close request manager if we own it
            if self._owns_request_manager:
                await self.request_manager.close()

            # Fire on_run_complete callback
            if self.on_run_complete:
                await self.on_run_complete(
                    scraper_name,
                    status,
                    error,
                )

    async def _worker(self, worker_id: int) -> None:
        """Worker coroutine that processes requests from the queue.

        Args:
            worker_id: Identifier for this worker (for debugging).
        """
        while True:
            # Check for graceful shutdown before getting next request
            if self.stop_event and self.stop_event.is_set():
                break

            # Get next request from queue (blocks until available)
            try:
                _priority, _counter, request = await self.request_queue.get()
            except asyncio.CancelledError:
                # Worker was cancelled (normal shutdown)
                break

            try:
                # Use match/case for exhaustive request type handling
                match request:
                    case (
                        NavigatingRequest()
                        | NonNavigatingRequest()
                        | ArchiveRequest()
                    ):
                        # Normal request flow
                        # Wrap request resolution to catch transient exceptions
                        try:
                            response: Response = (
                                await self.resolve_archive_request(request)
                                if isinstance(request, ArchiveRequest)
                                else await self.resolve_request(request)
                            )
                        except TransientException as e:
                            # Handle transient errors via callback
                            if self.on_transient_exception:
                                should_continue = (
                                    await self.on_transient_exception(e)
                                )
                                if not should_continue:
                                    break
                                continue
                            else:
                                raise
                        except RequestFailedHalt:
                            raise
                        except RequestFailedSkip:
                            # Skip this request silently and continue to next
                            continue

                        # Track speculation outcome if this is a speculative request
                        if request.is_speculative:
                            await self._track_speculation_outcome(
                                request, response
                            )

                        # Handle Callable continuations (convert to string)
                        continuation_name = (
                            request.continuation
                            if isinstance(request.continuation, str)
                            else getattr(
                                request.continuation,
                                "__name__",
                                str(request.continuation),
                            )
                        )

                        continuation_method = self.scraper.get_continuation(
                            continuation_name
                        )

                        # Process the generator
                        gen = continuation_method(response)
                        await self._process_generator(gen, response, request)

                    case _:
                        # Exhaustive match - should never reach here
                        assert_never(request)  # type: ignore[arg-type]
            finally:
                # Always mark task as done to allow join() to complete
                self.request_queue.task_done()

    async def enqueue_request(
        self, new_request: BaseRequest, context: Response | BaseRequest
    ) -> None:
        """Enqueue a new request, resolving it from the given context.

        Check for duplicates using duplicate_check callback before enqueuing.

        For NavigatingRequest yields: context is the Response
        For NonNavigatingRequest yields: context is the originating request
        For ArchiveRequest yields: context is the Response

        Args:
            new_request: The new request to enqueue.
            context: Response or originating request for URL resolution.
        """
        # Use the request's resolve_from method with the appropriate context
        resolved_request = new_request.resolve_from(context)  # type: ignore

        # Check for duplicates before enqueuing
        dedup_key = resolved_request.deduplication_key
        match dedup_key:
            case None:
                pass
            case SkipDeduplicationCheck():
                pass
            case str():
                if self.duplicate_check and not await self.duplicate_check(
                    dedup_key
                ):
                    return

        # Push onto queue with priority and counter for stable ordering
        async with self._queue_lock:
            await self.request_queue.put(
                (
                    resolved_request.priority,
                    self._queue_counter,
                    resolved_request,
                )
            )
            self._queue_counter += 1

    async def resolve_request(self, request: BaseRequest) -> Response:
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
        # Simply delegate to request manager - exception handling is done
        # by the driver's worker (LocalDevDriver._db_worker handles retries)
        response = await self.request_manager.resolve_request(request)
        return response

    async def resolve_archive_request(
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
        http_response = await self.resolve_request(request)

        # Use on_archive callback to save the file
        file_url = await self.on_archive(
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

    async def handle_data(self, data: ScraperReturnDatatype) -> None:
        # Validate deferred data if present
        if isinstance(data, DeferredValidation):
            try:
                validated_data: ScraperReturnDatatype = (
                    data.confirm()
                )  # ty: ignore[invalid-assignment]
                # Increment data counter on successful validation
                # Validation succeeded - send to on_data callback
                if self.on_data:
                    await self.on_data(validated_data)
            except DataFormatAssumptionException:
                # Validation failed - use callback hierarchy
                if self.on_invalid_data:
                    await self.on_invalid_data(data)
                else:
                    # No callbacks - re-raise the exception
                    raise
        else:
            # Increment data counter for non-validated data
            # Not deferred validation - invoke callback if provided
            if self.on_data:
                await self.on_data(data)

    async def _process_generator(
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
                        await self.handle_data(item.unwrap())
                    case EstimateData():
                        pass
                    case NavigatingRequest():
                        await self.enqueue_request(item, response)
                    case NonNavigatingRequest() | ArchiveRequest():
                        await self.enqueue_request(item, parent_request)
                    case None:
                        pass
                    case _:
                        assert_never(item)
        except HTMLStructuralAssumptionException as e:
            # Handle structural errors via callback
            if self.on_structural_error:
                should_continue = await self.on_structural_error(e)
                if not should_continue:
                    return
            else:
                raise
