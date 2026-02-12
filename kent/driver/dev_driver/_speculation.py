"""SpeculationMixin - @speculate decorator support."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from kent.common.decorators import (
    SpeculateMetadata,
    get_entry_metadata,
)
from kent.common.searchable import (
    SpeculateFunctionConfig,
)
from kent.data_types import (
    BaseRequest,
    BaseScraper,
    Response,
)
from kent.driver.dev_driver.sql_manager import SQLManager
from kent.driver.sync_driver import SpeculationState

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SpeculationMixin:
    """Speculation support: discovery, seeding, tracking, and recovery.

    Handles adaptive speculation for pagination/enumeration via the
    @speculate decorator pattern.
    """

    db: SQLManager
    scraper: BaseScraper
    _speculation_state: dict[str, SpeculationState]
    _speculation_lock: asyncio.Lock

    if TYPE_CHECKING:

        async def _emit_progress(
            self, event_type: str, data: dict[str, Any]
        ) -> None: ...

        def _serialize_request(
            self, request: BaseRequest
        ) -> dict[str, Any]: ...

        async def _mark_request_completed(self, request_id: int) -> None: ...

    # --- Discovery & Initialization ---

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

    async def _load_speculation_state_from_db(self) -> None:
        """Load persisted speculation state from DB for resumption.

        Updates self._speculation_state with any persisted state.
        """
        saved_states = await self.db.load_all_speculation_states()

        for func_name, saved in saved_states.items():
            if func_name in self._speculation_state:
                spec_state = self._speculation_state[func_name]
                spec_state.highest_successful_id = saved[
                    "highest_successful_id"
                ]
                spec_state.consecutive_failures = saved["consecutive_failures"]
                spec_state.current_ceiling = saved["current_ceiling"]
                spec_state.stopped = bool(saved["stopped"])

    # --- Queue Seeding ---

    async def _seed_speculative_queue(self) -> None:
        """Seed the queue with initial speculative requests based on params config.

        For each @speculate function:
        - If definite_range is configured, use that range
        - Otherwise, use (1, highest_observed) from decorator metadata
        - Enqueue requests for all IDs in the range

        When resuming, skips IDs that have already been processed (based on
        current_ceiling from persisted state).
        """
        for func_name, spec_state in self._speculation_state.items():
            if spec_state.stopped:
                # Speculation was stopped in previous run, skip
                continue

            # Get the speculate function
            func = getattr(self.scraper, func_name)

            # Determine the range
            if spec_state.config.definite_range is not None:
                start, end = spec_state.config.definite_range
            else:
                # Use defaults from decorator metadata
                start = 1
                end = spec_state.metadata.highest_observed

            # If resuming, start from current_ceiling + 1
            if spec_state.current_ceiling > 0:
                start = max(start, spec_state.current_ceiling + 1)
                if start > end:
                    # Already processed all IDs in range
                    continue

            # Seed the queue
            for id_value in range(start, end + 1):
                request = func(id_value)
                # Ensure speculative fields are set
                request = request.speculative(func_name, id_value)

                # Serialize and enqueue via DB
                request_data = self._serialize_request(request)
                await self.db.insert_request(
                    priority=request.priority,
                    request_type=request_data["request_type"],
                    method=request_data["method"],
                    url=request_data["url"],
                    headers_json=request_data["headers_json"],
                    cookies_json=request_data["cookies_json"],
                    body=request_data["body"],
                    continuation=request_data["continuation"],
                    current_location=request_data["current_location"],
                    accumulated_data_json=request_data[
                        "accumulated_data_json"
                    ],
                    aux_data_json=request_data["aux_data_json"],
                    permanent_json=request_data["permanent_json"],
                    expected_type=request_data["expected_type"],
                    dedup_key=None,
                    parent_id=None,
                    is_speculative=request_data["is_speculative"],
                    speculation_id=request_data["speculation_id"],
                )

            # Update current_ceiling to the highest seeded ID
            spec_state.current_ceiling = end

    # --- Dynamic Extension ---

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
            for id_value in range(
                spec_state.current_ceiling + 1, new_ceiling + 1
            ):
                request = func(id_value)
                # Ensure speculative fields are set
                request = request.speculative(func_name, id_value)

                # Serialize and enqueue via DB
                request_data = self._serialize_request(request)
                await self.db.insert_request(
                    priority=request.priority,
                    request_type=request_data["request_type"],
                    method=request_data["method"],
                    url=request_data["url"],
                    headers_json=request_data["headers_json"],
                    cookies_json=request_data["cookies_json"],
                    body=request_data["body"],
                    continuation=request_data["continuation"],
                    current_location=request_data["current_location"],
                    accumulated_data_json=request_data[
                        "accumulated_data_json"
                    ],
                    aux_data_json=request_data["aux_data_json"],
                    permanent_json=request_data["permanent_json"],
                    expected_type=request_data["expected_type"],
                    dedup_key=None,
                    parent_id=None,
                    is_speculative=request_data["is_speculative"],
                    speculation_id=request_data["speculation_id"],
                )

            spec_state.current_ceiling = new_ceiling

    # --- Outcome Tracking ---

    async def _track_speculation_outcome(
        self, request: BaseRequest, response: Response
    ) -> None:
        """Track the outcome of a speculative request.

        Updates highest_successful_id and consecutive_failures based on response.
        Persists state to DB after update.

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

            # Persist state to DB
            await self.db.save_speculation_state(
                func_name=spec_state.func_name,
                highest_successful_id=spec_state.highest_successful_id,
                consecutive_failures=spec_state.consecutive_failures,
                current_ceiling=spec_state.current_ceiling,
                stopped=spec_state.stopped,
            )

    # --- Start ID Application ---

    async def _apply_speculative_start_ids(self) -> None:
        """Apply speculative start IDs from database to scraper params.

        This is used by the restart-speculative feature. When the user sets
        speculative start IDs via the web UI (stored in the speculative_start_ids
        table), those values are applied to the scraper's params when the driver
        starts running.

        After applying, the start IDs are cleared from the database to ensure
        they only take effect once.
        """
        # Get start IDs from database
        start_ids = await self.db.get_speculative_start_ids()
        if not start_ids:
            return

        # Ensure scraper has params
        if (
            not hasattr(self.scraper, "_params")
            or self.scraper._params is None
        ):
            # Initialize params using the class method
            self.scraper._params = self.scraper.__class__.params()

        # Apply start IDs to speculative proxy
        for step_name, starting_id in start_ids.items():
            try:
                setattr(
                    self.scraper._params.speculative, step_name, starting_id
                )
                logger.info(
                    f"Applied speculative start ID: {step_name} = {starting_id}"
                )
            except AttributeError:
                logger.warning(
                    f"Unknown speculative step: {step_name}, skipping"
                )

        # Clear the start IDs after applying (one-time use)
        await self.db.clear_all_speculative_start_ids()

    # --- Progress Tracking ---

    async def get_speculative_progress(self, step_name: str) -> int | None:
        """Get the highest_successful_id for a speculative step.

        Args:
            step_name: The name of the speculative step method.

        Returns:
            The highest_successful_id, or None if no progress recorded.
        """
        state = await self.db.load_speculation_state(step_name)
        if state is None:
            return None
        return state["highest_successful_id"]

    async def get_all_speculative_progress(self) -> dict[str, int]:
        """Get all speculative progress entries.

        Returns:
            Dict mapping step names to their highest_successful_id.
        """
        return await self.db.get_all_speculation_progress()

    # --- Recovery ---

    async def _recover_speculative_step(
        self,
        request_id: int,
        step_name: str,
        current_speculative_id: int,
    ) -> None:
        """Recover a speculative step by re-invoking it from the latest ID.

        Called when a speculative request is processed but its generator context
        has been lost (e.g., after server restart). This re-invokes the original
        step with the latest speculative_id from the progress table.

        Args:
            request_id: The database ID of the request being processed.
            step_name: The name of the speculative step method.
            current_speculative_id: The speculative_id from the current request.
        """
        # Get the latest progress for this step from speculation_tracking
        latest_id = await self.get_speculative_progress(step_name)

        # Use the maximum of current request ID and stored progress
        # This handles cases where progress wasn't stored yet
        recovery_id = max(current_speculative_id, latest_id or 0)

        # Progress is tracked via save_speculation_state in _track_speculation_outcome

        logger.info(
            f"Recovering speculative step '{step_name}': "
            f"processed ID {current_speculative_id}, "
            f"will restart from {recovery_id + 1}"
        )

        # Get the step continuation and re-invoke it with the recovery ID
        # We need to build a fake Response to start the step
        # The step will be called via get_entry which starts fresh
        try:
            # Set the speculative starting ID in params for recovery
            if self.scraper._params is not None:
                try:
                    setattr(
                        self.scraper._params.speculative,
                        step_name,
                        recovery_id + 1,
                    )
                    logger.info(
                        f"Set params.speculative.{step_name} = {recovery_id + 1} "
                        f"for recovery"
                    )
                except AttributeError:
                    logger.warning(
                        f"Could not set speculative starting ID for {step_name} - "
                        f"step may not be configured in params"
                    )

            # Re-invoke the entry point to restart the speculative flow
            # This will call get_entry() which should yield the NavigatingRequest
            # that triggers the speculative step
            await self._emit_progress(
                "speculative_recovery_initiated",
                {
                    "step_name": step_name,
                    "processed_id": current_speculative_id,
                    "recovery_id": recovery_id + 1,
                },
            )

        except Exception as e:
            logger.exception(
                f"Failed to recover speculative step {step_name}: {e}"
            )

        # Mark the original request as completed (we've initiated recovery)
        await self._mark_request_completed(request_id)
