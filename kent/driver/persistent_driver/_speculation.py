"""SpeculationMixin - Speculative protocol support for the persistent driver."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

from kent.data_types import BaseRequest
from kent.driver._speculation_support import (
    AsyncSpeculationSupport,
    SpeculationState,
)
from kent.driver.persistent_driver.sql_manager import SQLManager

if TYPE_CHECKING:
    pass

logger = logging.getLogger(__name__)


class SpeculationMixin(AsyncSpeculationSupport):
    """Speculation support: discovery, seeding, tracking, and extension.

    Extends :class:`AsyncSpeculationSupport` with DB-backed request
    dispatch and state persistence so progress survives restarts.
    """

    db: SQLManager

    if TYPE_CHECKING:

        async def _emit_progress(
            self, event_type: str, data: dict[str, Any]
        ) -> None: ...

        def _serialize_request(
            self, request: BaseRequest
        ) -> dict[str, Any]: ...

        async def _mark_request_completed(self, request_id: int) -> None: ...

    # --- Hook implementations ---

    async def _enqueue_speculative(self, request: BaseRequest) -> None:
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
            accumulated_data_json=request_data["accumulated_data_json"],
            permanent_json=request_data["permanent_json"],
            expected_type=request_data["expected_type"],
            dedup_key=None,
            parent_id=None,
            is_speculative=request_data["is_speculative"],
            speculation_id=request_data["speculation_id"],
            verify=request_data.get("verify"),
        )

    async def _after_outcome(self, spec_state: SpeculationState) -> None:
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

    # --- Persistent-only: resume state from DB ---

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
