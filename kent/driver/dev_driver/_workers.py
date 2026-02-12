"""WorkerMixin - Worker management and request processing loop."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from kent.common.exceptions import (
    RequestFailedHalt,
    RequestFailedSkip,
    TransientException,
)
from kent.data_types import (
    ArchiveRequest,
    BaseRequest,
    BaseScraper,
    NavigatingRequest,
    NonNavigatingRequest,
    Response,
    ScraperYield,
)
from kent.driver.dev_driver.sql_manager import SQLManager
from kent.driver.sync_driver import SpeculationState

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Generator

    from kent.common.deferred_validation import DeferredValidation
    from kent.common.exceptions import (
        HTMLStructuralAssumptionException,
    )
    from kent.data_types import ArchiveResponse

logger = logging.getLogger(__name__)


class WorkerMixin:
    """Worker lifecycle, dynamic scaling, and request processing.

    Provides the main worker loop (_db_worker), worker scaling (_worker_monitor),
    and request processing (_process_regular_request, _process_generator_with_storage).
    """

    db: SQLManager
    scraper: BaseScraper
    stop_event: asyncio.Event
    max_workers: int
    num_workers: int
    request_manager: Any
    _worker_tasks: dict[int, asyncio.Task[None]]
    _next_worker_id: int
    _speculation_state: dict[str, SpeculationState]
    # Callback attrs — defined on AsyncDriver, annotated here for mypy
    on_progress: Callable[..., Awaitable[None]] | None
    on_invalid_data: Callable[[DeferredValidation], Awaitable[None]] | None
    on_structural_error: (
        Callable[[HTMLStructuralAssumptionException], Awaitable[bool]] | None
    )

    if TYPE_CHECKING:

        async def _emit_progress(
            self, event_type: str, data: dict[str, Any]
        ) -> None: ...

        # Provided by QueueMixin
        async def _get_next_request(
            self,
        ) -> tuple[int, BaseRequest] | None: ...

        async def enqueue_request(
            self,
            new_request: BaseRequest,
            context: Response | BaseRequest,
            parent_request_id: int | None = None,
        ) -> None: ...

        # Provided by StorageMixin
        async def _mark_request_completed(self, request_id: int) -> None: ...

        async def _mark_request_failed(
            self, request_id: int, error_message: str
        ) -> None: ...

        async def _handle_retry(
            self, request_id: int, error: Exception
        ) -> bool: ...

        async def _store_response(
            self,
            request_id: int,
            response: Response,
            continuation: str,
            speculation_outcome: str | None = None,
        ) -> int: ...

        async def _store_result(
            self,
            request_id: int,
            data: Any,
            is_valid: bool = True,
            validation_errors: list[dict[str, Any]] | None = None,
        ) -> int: ...

        # Provided by SpeculationMixin
        async def _track_speculation_outcome(
            self, request: BaseRequest, response: Response
        ) -> None: ...

        # Provided by AsyncDriver
        async def resolve_request(self, request: BaseRequest) -> Response: ...

        async def resolve_archive_request(
            self, request: ArchiveRequest
        ) -> ArchiveResponse: ...

        async def handle_data(self, data: Any) -> None: ...

    # --- Worker Management ---

    @property
    def active_worker_count(self) -> int:
        """Number of currently active workers."""
        return sum(1 for t in self._worker_tasks.values() if not t.done())

    def _spawn_worker(self) -> int:
        """Spawn a new worker and return its ID.

        Returns:
            The worker ID of the newly spawned worker.
        """
        worker_id = self._next_worker_id
        self._next_worker_id += 1
        task = asyncio.create_task(self._db_worker(worker_id))
        self._worker_tasks[worker_id] = task

        # Clean up when worker exits
        def on_worker_done(
            _: asyncio.Task[None], wid: int = worker_id
        ) -> None:
            self._worker_tasks.pop(wid, None)

        task.add_done_callback(on_worker_done)

        logger.info(
            f"Spawned worker {worker_id}, total active: {self.active_worker_count}"
        )
        return worker_id

    async def _worker_monitor(self) -> None:
        """Monitor task that dynamically scales workers based on conditions.

        Adds a worker if:
        - There are pending requests
        - The rate limit > 2 * active_worker_count
        - active_worker_count < max_workers

        Exits when:
        - stop_event is set, OR
        - active_worker_count == 0 and no pending requests
        """
        logger.info(
            f"Worker monitor started (max_workers={self.max_workers}, "
            f"poll_interval=60s)"
        )

        while not self.stop_event.is_set():
            # Wait 60 seconds between checks
            try:
                await asyncio.wait_for(self.stop_event.wait(), timeout=60.0)
                # If we get here, stop_event was set
                break
            except asyncio.TimeoutError:
                # Normal timeout - proceed with check
                pass

            # Check exit condition: no workers and no pending work
            active_count = self.active_worker_count
            pending_count = await self.db.count_pending_requests()

            if active_count == 0 and pending_count == 0:
                logger.info(
                    "Worker monitor exiting: no workers and no pending requests"
                )
                break

            # Check scaling conditions
            if pending_count == 0:
                logger.debug(
                    f"Worker monitor: no pending requests "
                    f"(active_workers={active_count})"
                )
                continue

            if active_count >= self.max_workers:
                logger.debug(
                    f"Worker monitor: at max workers "
                    f"({active_count}/{self.max_workers})"
                )
                continue

            # Get current rate from the ATB rate limiter
            current_rate = getattr(self.request_manager, "_rate", 0.0)

            # Scale if rate > 2 * active_workers
            if current_rate > 2 * active_count:
                new_worker_id = self._spawn_worker()
                logger.info(
                    f"Worker monitor: scaled up to {self.active_worker_count} workers "
                    f"(rate={current_rate:.2f}/s, pending={pending_count})"
                )

                await self._emit_progress(
                    "worker_scaled",
                    {
                        "worker_id": new_worker_id,
                        "active_workers": self.active_worker_count,
                        "current_rate": current_rate,
                        "pending_requests": pending_count,
                    },
                )
            else:
                logger.debug(
                    f"Worker monitor: rate ({current_rate:.2f}/s) <= "
                    f"2 * workers ({2 * active_count}), no scale-up"
                )

        logger.info("Worker monitor stopped")

    # --- Request Processing ---

    async def _db_worker(self, worker_id: int) -> None:
        """Worker that processes requests from the database queue.

        Handles regular requests (NavigatingRequest, NonNavigatingRequest, ArchiveRequest).
        Speculative requests are handled via the new @speculate decorator pattern.

        Args:
            worker_id: Identifier for this worker.
        """
        import time as time_module

        logger.info(f"[W{worker_id}] Worker started")
        requests_processed = 0

        while True:
            loop_start = time_module.time()

            # Check for graceful shutdown
            if self.stop_event.is_set():
                logger.info(
                    f"[W{worker_id}] Exiting: stop_event set (processed {requests_processed} requests)"
                )
                break

            # Get next request from DB
            result = await self._get_next_request()

            if result is None:
                # No immediately available requests - check for scheduled retries
                retry_delay = await self.db.get_next_scheduled_retry_delay()

                if retry_delay is not None and retry_delay > 0:
                    # There are scheduled retries - wait for the next one
                    # Add a small buffer and cap at a reasonable max wait
                    wait_time = min(retry_delay + 0.1, 60.0)
                    logger.info(
                        f"[W{worker_id}] Waiting {wait_time:.1f}s for scheduled retry"
                    )
                    await asyncio.sleep(wait_time)

                    # Check for shutdown after waiting
                    if self.stop_event.is_set():
                        break

                    # Try again after waiting
                    result = await self._get_next_request()
                    if result is None:
                        # Still nothing - continue loop to check again
                        continue
                else:
                    # No scheduled retries - poll for new work
                    # Other workers may still be processing and generating new requests
                    # Poll at moderate rate (100ms) to balance responsiveness and DB load
                    consecutive_empty = 0
                    max_polls = 100  # 10 seconds max polling

                    for poll_attempt in range(max_polls):
                        # Wait before retry (100ms gives good balance)
                        await asyncio.sleep(0.1)

                        # Check for shutdown
                        if self.stop_event.is_set():
                            logger.info(
                                f"[W{worker_id}] Stop event during polling"
                            )
                            break

                        # Try to get work - this is the only DB call per iteration
                        result = await self._get_next_request()
                        if result is not None:
                            logger.info(
                                f"[W{worker_id}] Found work after {poll_attempt + 1} polls"
                            )
                            break

                        # Check exit condition periodically (every 0.5s)
                        if poll_attempt % 5 == 4:
                            in_progress_count = (
                                await self.db.count_in_progress()
                            )
                            pending_count = (
                                await self.db.count_pending_requests()
                            )

                            if in_progress_count == 0 and pending_count == 0:
                                consecutive_empty += 1
                                if (
                                    consecutive_empty >= 6
                                ):  # ~3 seconds of true idle
                                    logger.info(
                                        f"[W{worker_id}] Exiting: idle (processed {requests_processed})"
                                    )
                                    break
                            else:
                                consecutive_empty = 0

                            if poll_attempt % 20 == 19:
                                logger.info(
                                    f"[W{worker_id}] Polling... in_progress={in_progress_count}, pending={pending_count}"
                                )

                    if result is None:
                        logger.info(
                            f"[W{worker_id}] Exiting: queue empty after polling (processed {requests_processed} requests)"
                        )
                        break

            request_id, request = result
            logger.debug(f"[W{worker_id}] Dequeued request {request_id}")

            try:
                await self._emit_progress(
                    "request_started",
                    {
                        "request_id": request_id,
                        "url": request.request.url,
                        "continuation": request.continuation,
                    },
                )

                # Get continuation name
                continuation_name = (
                    request.continuation
                    if isinstance(request.continuation, str)
                    else request.continuation.__name__
                )

                # Process the request
                req_start = time_module.time()
                await self._process_regular_request(
                    request_id, request, continuation_name
                )
                req_time = time_module.time() - req_start
                loop_time = time_module.time() - loop_start
                requests_processed += 1
                logger.info(
                    f"[W{worker_id}] Completed request {request_id} in {req_time * 1000:.1f}ms (loop={loop_time * 1000:.1f}ms, total={requests_processed})"
                )

            except RequestFailedHalt:
                # User callback requested halt - propagate up
                raise

            except RequestFailedSkip:
                # User callback requested skip - mark as failed and continue
                await self._mark_request_failed(
                    request_id, "Skipped by on_transient_exception callback"
                )
                await self._emit_progress(
                    "request_skipped",
                    {
                        "request_id": request_id,
                        "url": request.request.url,
                        "reason": "callback_requested_skip",
                    },
                )
                continue

            except TransientException as e:
                should_retry = await self._handle_retry(request_id, e)
                if should_retry:
                    # Log at warning level without full traceback for transient errors
                    logger.warning(
                        f"Worker {worker_id} transient error on request "
                        f"{request_id}: {type(e).__name__}: {e}"
                    )
                    await self._emit_progress(
                        "request_retry_scheduled",
                        {
                            "request_id": request_id,
                            "url": request.request.url,
                            "error": str(e),
                            "error_type": type(e).__name__,
                        },
                    )
                    continue  # Don't store as error, will be retried
                else:
                    # Max backoff exceeded - log the full traceback and mark failed
                    logger.exception(
                        f"Worker {worker_id} transient error exceeded max "
                        f"backoff for request {request_id}"
                    )

                    # Mark as failed and store error
                    await self._mark_request_failed(request_id, str(e))

                    from kent.driver.dev_driver.errors import (
                        store_error,
                    )

                    await store_error(
                        self.db._session_factory,
                        e,
                        request_id=request_id,
                        request_url=request.request.url,
                    )

                    await self._emit_progress(
                        "request_failed",
                        {
                            "request_id": request_id,
                            "url": request.request.url,
                            "error": str(e),
                            "error_type": type(e).__name__,
                            "reason": "max_backoff_exceeded",
                        },
                    )

            except Exception as e:
                # Non-transient error - log full traceback
                logger.exception(
                    f"Worker {worker_id} error processing request {request_id}"
                )

                # Non-transient error or max backoff exceeded - mark as failed
                await self._mark_request_failed(request_id, str(e))

                # Store error in database for tracking and requeue
                from kent.driver.dev_driver.errors import (
                    store_error,
                )

                await store_error(
                    self.db._session_factory,
                    e,
                    request_id=request_id,
                    request_url=request.request.url,
                )

                await self._emit_progress(
                    "request_failed",
                    {
                        "request_id": request_id,
                        "url": request.request.url,
                        "error": str(e),
                        "error_type": type(e).__name__,
                    },
                )

    async def _process_regular_request(
        self,
        request_id: int,
        request: BaseRequest,
        continuation_name: str,
    ) -> None:
        """Process a regular (non-speculative, non-resume) request.

        Args:
            request_id: Database ID of the request.
            request: The request to process.
            continuation_name: Name of the continuation method.
        """
        # Process the request using parent class methods
        # For ArchiveRequest, resolve_archive_request returns ArchiveResponse
        # which is a subclass of Response with a file_url field
        from kent.data_types import ArchiveResponse

        logger.info(f"Request {request_id}: starting HTTP fetch")
        response: Response = (
            await self.resolve_archive_request(request)
            if isinstance(request, ArchiveRequest)
            else await self.resolve_request(request)
        )
        logger.info(
            f"Request {request_id}: HTTP fetch complete, status={response.status_code}"
        )

        # Track speculation outcome for @speculate requests
        if request.is_speculative and self._speculation_state:
            await self._track_speculation_outcome(request, response)

        # Verify ArchiveResponse for ArchiveRequest
        if isinstance(request, ArchiveRequest) and not isinstance(
            response, ArchiveResponse
        ):
            logger.error(
                f"Expected ArchiveResponse for ArchiveRequest, got {type(response)}"
            )

        # Store the response in the database
        await self._store_response(request_id, response, continuation_name)

        # Get continuation method and process generator
        continuation_method = self.scraper.get_continuation(continuation_name)
        gen = continuation_method(response)

        await self._process_generator_with_storage(
            gen, response, request, continuation_name, request_id
        )

        # Mark completed
        await self._mark_request_completed(request_id)

        await self._emit_progress(
            "request_completed",
            {
                "request_id": request_id,
                "url": request.request.url,
            },
        )

    async def _process_generator_with_storage(
        self,
        gen: Generator[ScraperYield, bool | None, None],
        response: Response,
        parent_request: BaseRequest,
        continuation_name: str,
        request_id: int,
    ) -> None:
        """Process generator with DB storage.

        Uses simple iteration (for item in gen).

        Args:
            gen: The generator from the continuation method.
            response: The Response that triggered this continuation.
            parent_request: The request that initiated this continuation.
            continuation_name: Name of the continuation method.
            request_id: Database ID for result storage.
        """
        from kent.common.deferred_validation import (
            DeferredValidation,
        )
        from kent.common.exceptions import (
            DataFormatAssumptionException,
            HTMLStructuralAssumptionException,
        )
        from kent.data_types import EstimateData, ParsedData

        try:
            for item in gen:
                match item:
                    case ParsedData():
                        raw_data = item.unwrap()
                        # Handle deferred validation
                        if isinstance(raw_data, DeferredValidation):
                            try:
                                validated_data = raw_data.confirm()
                                await self._store_result(
                                    request_id, validated_data
                                )
                                await self.handle_data(validated_data)
                            except DataFormatAssumptionException as e:
                                await self._store_result(
                                    request_id,
                                    e.failed_doc,
                                    is_valid=False,
                                    validation_errors=e.errors,
                                )
                                if self.on_invalid_data:
                                    await self.on_invalid_data(raw_data)
                        else:
                            await self._store_result(request_id, raw_data)
                            await self.handle_data(raw_data)

                    case EstimateData():
                        import json as _json

                        types_json = _json.dumps(
                            [t.__name__ for t in item.expected_types]
                        )
                        await self.db.store_estimate(
                            request_id=request_id,
                            expected_types_json=types_json,
                            min_count=item.min_count,
                            max_count=item.max_count,
                        )

                    case NavigatingRequest():
                        await self.enqueue_request(item, response, request_id)

                    case NonNavigatingRequest() | ArchiveRequest():
                        await self.enqueue_request(
                            item, parent_request, request_id
                        )

                    case None:
                        pass

        except HTMLStructuralAssumptionException as e:
            if self.on_structural_error:
                should_continue = await self.on_structural_error(e)
                if not should_continue:
                    return
            else:
                raise
