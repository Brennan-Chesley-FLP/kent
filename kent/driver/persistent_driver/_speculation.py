"""SpeculationMixin - Speculative protocol support for the persistent driver."""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING, Any

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
    """Speculation support: discovery, seeding, tracking, and extension.

    Handles adaptive speculation for entry points whose parameter
    implements the Speculative protocol.
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
        """Discover speculative entry functions and build tracking state.

        Looks up templates from ``scraper._speculation_templates`` (populated
        by ``initial_seed()``). Each template at index *i* becomes a
        ``SpeculationState`` keyed by ``{func_name}:{i}``.

        Returns:
            Dictionary mapping state keys to their SpeculationState.
        """
        state: dict[str, SpeculationState] = {}
        templates = getattr(self.scraper, "_speculation_templates", {})

        for entry_info in self.scraper.list_speculative_entries():
            func_templates = templates.get(entry_info.name, [])
            for i, template in enumerate(func_templates):
                key = f"{entry_info.name}:{i}"
                state[key] = SpeculationState(
                    func_name=key,
                    template=template,
                    param_index=i,
                    base_func_name=entry_info.name,
                )
        return state

    async def _load_speculation_state_from_db(self) -> None:
        """Load persisted speculation state from DB for resumption.

        Updates self._speculation_state with any persisted state.
        Also reconstructs templates from stored template_json.
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
                spec_state.stopped = saved["stopped"]
            elif "template_json" in saved and saved["template_json"]:
                # State exists in DB but not in current discovery.
                # Try to reconstruct from template_json if possible.
                # This handles resume when initial_seed() wasn't called.
                param_index = saved["param_index"]
                # Extract base func name from key (format: "func_name:index")
                base_name = (
                    func_name.rsplit(":", 1)[0]
                    if ":" in func_name
                    else func_name
                )

                # Find the param type for deserialization
                param_type = None
                for entry_info in self.scraper.list_speculative_entries():
                    if (
                        entry_info.name == base_name
                        and entry_info.speculative_param
                    ):
                        param_type = entry_info.param_types[
                            entry_info.speculative_param
                        ]
                        break

                if param_type is not None and hasattr(
                    param_type, "model_validate_json"
                ):
                    try:
                        template = param_type.model_validate_json(
                            saved["template_json"]
                        )
                        spec_state = SpeculationState(
                            func_name=func_name,
                            template=template,
                            param_index=param_index,
                            base_func_name=base_name,
                            highest_successful_id=saved[
                                "highest_successful_id"
                            ],
                            consecutive_failures=saved["consecutive_failures"],
                            current_ceiling=saved["current_ceiling"],
                            stopped=saved["stopped"],
                        )
                        self._speculation_state[func_name] = spec_state
                    except Exception:
                        logger.warning(
                            f"Failed to deserialize template for {func_name}, skipping"
                        )

    # --- Queue Seeding ---

    async def _seed_speculative_queue(self) -> None:
        """Seed the queue with requests from speculative templates.

        Every enqueued request is speculative (``is_speculative=True`` +
        populated ``speculation_id``). Seeding is split into two passes:
        first ``template.seed_range()`` (the explicitly-requested IDs),
        then — when ``template.should_advance`` is True and
        ``template.max_gap() > 0`` — an initial advance window of
        ``max_gap()`` probes past ``seed_range.stop``.

        When resuming, skips IDs at or below ``current_ceiling``.
        """
        for state_key, spec_state in self._speculation_state.items():
            if spec_state.stopped:
                continue

            func = getattr(self.scraper, spec_state.base_func_name)
            template = spec_state.template
            speculative_param = None
            for entry_info in self.scraper.list_speculative_entries():
                if entry_info.name == spec_state.base_func_name:
                    speculative_param = entry_info.speculative_param
                    break
            assert speculative_param is not None

            seed_ids = template.seed_range()
            # When resuming, skip anything at or below current_ceiling.
            resume_floor = (
                spec_state.current_ceiling + 1
                if spec_state.current_ceiling > 0
                else seed_ids.start
            )
            seed_ids_to_run = [n for n in seed_ids if n >= resume_floor]

            # Advance floor: one past the last seed id, falling back to
            # the template's floor when seed_range is empty. Resume bias
            # wins when we're past that point already.
            advance_floor = max(seed_ids.start, seed_ids.stop, resume_floor)
            window = (
                range(advance_floor, advance_floor + template.max_gap())
                if template.should_advance and template.max_gap() > 0
                else range(0)
            )

            for n in seed_ids_to_run + list(window):
                concrete = template.from_int(n)
                request = func(**{speculative_param: concrete})
                request = request.speculative(
                    state_key, spec_state.param_index, n
                )
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
                    permanent_json=request_data["permanent_json"],
                    expected_type=request_data["expected_type"],
                    dedup_key=None,
                    parent_id=None,
                    is_speculative=request_data["is_speculative"],
                    speculation_id=request_data["speculation_id"],
                    verify=request_data.get("verify"),
                )

            if window:
                spec_state.current_ceiling = (
                    advance_floor + template.max_gap() - 1
                )
            else:
                spec_state.current_ceiling = advance_floor - 1
                spec_state.stopped = True

    # --- Dynamic Extension ---

    async def _extend_speculation(self, state_key: str) -> None:
        """Extend speculation when approaching the ceiling.

        Does not extend if stopped or max_gap == 0 (frozen).

        Args:
            state_key: Key in _speculation_state.
        """
        spec_state = self._speculation_state.get(state_key)
        if spec_state is None or spec_state.stopped:
            return

        gap = spec_state.template.max_gap()
        if gap == 0:
            return

        if spec_state.consecutive_failures >= gap:
            spec_state.stopped = True
            return

        if (
            spec_state.highest_successful_id
            >= spec_state.current_ceiling - gap
        ):
            func = getattr(self.scraper, spec_state.base_func_name)
            speculative_param = None
            for entry_info in self.scraper.list_speculative_entries():
                if entry_info.name == spec_state.base_func_name:
                    speculative_param = entry_info.speculative_param
                    break

            new_ceiling = spec_state.current_ceiling + gap
            for n in range(spec_state.current_ceiling + 1, new_ceiling + 1):
                concrete = spec_state.template.from_int(n)
                assert speculative_param is not None
                request = func(**{speculative_param: concrete})
                request = request.speculative(
                    state_key, spec_state.param_index, n
                )

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
                    permanent_json=request_data["permanent_json"],
                    expected_type=request_data["expected_type"],
                    dedup_key=None,
                    parent_id=None,
                    is_speculative=request_data["is_speculative"],
                    speculation_id=request_data["speculation_id"],
                    verify=request_data.get("verify"),
                )

            spec_state.current_ceiling = new_ceiling

    # --- Outcome Tracking ---

    async def _track_speculation_outcome(
        self, request: BaseRequest, response: Response
    ) -> None:
        """Track the outcome of a speculative request.

        Updates highest_successful_id and consecutive_failures.
        Persists state to DB after update.
        """
        if not request.is_speculative or request.speculation_id is None:
            return

        state_key, _param_index, speculative_id = request.speculation_id
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
                    gap = spec_state.template.max_gap()
                    if spec_state.consecutive_failures >= gap:
                        spec_state.stopped = True

            # Persist state to DB
            template_json = None
            if hasattr(spec_state.template, "model_dump_json"):
                template_json = spec_state.template.model_dump_json()

            await self.db.save_speculation_state(
                func_name=spec_state.func_name,
                highest_successful_id=spec_state.highest_successful_id,
                consecutive_failures=spec_state.consecutive_failures,
                current_ceiling=spec_state.current_ceiling,
                stopped=spec_state.stopped,
                param_index=spec_state.param_index,
                template_json=template_json,
            )

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
