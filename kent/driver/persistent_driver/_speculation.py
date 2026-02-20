"""SpeculationMixin - @speculate decorator support."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

from kent.common.decorators import (
    SpeculateMetadata,
    _get_speculative_axis,
)
from kent.common.searchable import (
    SpeculateFunctionConfig,
)
from kent.common.speculation_types import (
    SimpleSpeculation,
    YearlySpeculation,
)
from kent.data_types import (
    BaseRequest,
    BaseScraper,
    Response,
)
from kent.driver.persistent_driver.sql_manager import SQLManager
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

        Uses BaseScraper.list_speculative_entries() to find speculative entries.

        For SimpleSpeculation: creates one SpeculationState keyed by func_name.
        For YearlySpeculation: creates one SpeculationState per year partition,
            keyed by ``func_name:year``.

        Returns:
            Dictionary mapping state keys to their SpeculationState.
        """
        from datetime import date as date_cls

        state: dict[str, SpeculationState] = {}
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
                if entry_info.name in overrides:
                    partitions = overrides[entry_info.name]
                else:
                    axis_name = _get_speculative_axis(entry_info.param_types)
                    partitions = [
                        {
                            "year": p.year,
                            axis_name: p.number,
                            "frozen": p.frozen,
                        }
                        for p in spec.backfill
                    ]

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

                # Trailing period
                prev_year = current_year - 1
                prev_key = f"{entry_info.name}:{prev_year}"
                jan1 = date_cls(current_year, 1, 1)
                if (
                    today - jan1
                ) < spec.trailing_period and prev_key not in state:
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
        """Seed the queue with initial speculative requests.

        For SimpleSpeculation: calls func(id_value) for each ID in range.
        For YearlySpeculation: calls func(year, number) for each ID in range.

        When resuming, skips IDs that have already been processed (based on
        current_ceiling from persisted state).
        """
        for state_key, spec_state in self._speculation_state.items():
            if spec_state.stopped:
                continue

            func = getattr(self.scraper, spec_state.base_func_name)

            # Determine the range
            if spec_state.config.definite_range is not None:
                start, end = spec_state.config.definite_range
            elif spec_state.metadata is not None:
                start = 1
                end = spec_state.metadata.highest_observed
            else:
                continue

            # If resuming, start from current_ceiling + 1
            if spec_state.current_ceiling > 0:
                start = max(start, spec_state.current_ceiling + 1)
                if start > end:
                    continue

            # Seed the queue
            for id_value in range(start, end + 1):
                if spec_state.year is not None:
                    request = func(spec_state.year, id_value)
                else:
                    request = func(id_value)
                request = request.speculative(state_key, id_value)

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

            spec_state.current_ceiling = end
            if spec_state.frozen:
                spec_state.stopped = True

    # --- Dynamic Extension ---

    async def _extend_speculation(self, state_key: str) -> None:
        """Extend speculation for a partition when approaching the ceiling.

        Frozen partitions never extend.

        Args:
            state_key: Key in _speculation_state.
        """
        spec_state = self._speculation_state.get(state_key)
        if spec_state is None or spec_state.stopped or spec_state.frozen:
            return

        # Determine plus threshold
        if spec_state.config.plus is not None:
            plus = spec_state.config.plus
        elif isinstance(
            spec_state.speculation, SimpleSpeculation | YearlySpeculation
        ):
            plus = spec_state.speculation.largest_observed_gap
        else:
            return

        if spec_state.consecutive_failures >= plus:
            spec_state.stopped = True
            return

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
        """
        if not request.is_speculative or request.speculation_id is None:
            return

        state_key, speculative_id = request.speculation_id
        spec_state = self._speculation_state.get(state_key)
        if spec_state is None:
            return

        is_success = 200 <= response.status_code < 300
        if is_success and not self.scraper.fails_successfully(response):
            is_success = False

        async with self._speculation_lock:
            if is_success:
                if speculative_id > spec_state.highest_successful_id:
                    spec_state.highest_successful_id = speculative_id
                spec_state.consecutive_failures = 0
                await self._extend_speculation(state_key)
            else:
                if speculative_id > spec_state.highest_successful_id:
                    spec_state.consecutive_failures += 1
                    if spec_state.config.plus is not None:
                        plus = spec_state.config.plus
                    elif isinstance(
                        spec_state.speculation,
                        SimpleSpeculation | YearlySpeculation,
                    ):
                        plus = spec_state.speculation.largest_observed_gap
                    else:
                        return
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
            # This will call get_entry() which should yield the Request
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
