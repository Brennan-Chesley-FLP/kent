"""Speculation tracking operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy import delete, func, select
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.dev_driver.models import (
    SpeculationTracking,
    SpeculativeStartId,
)

if TYPE_CHECKING:
    import asyncio


class SpeculationMixin:
    """SpeculativeStartId and SpeculationTracking operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    # --- Speculative Start IDs (for restart-speculative feature) ---

    async def set_speculative_start_id(
        self, step_name: str, starting_id: int
    ) -> None:
        """Set a speculative starting ID for a step.

        Args:
            step_name: The name of the speculative step method.
            starting_id: The speculative_id to start from.
        """
        async with self._lock, self._session_factory() as session:
            stmt = sqlite_insert(SpeculativeStartId).values(
                step_name=step_name,
                starting_id=starting_id,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["step_name"],
                set_={
                    "starting_id": stmt.excluded.starting_id,
                    "updated_at": func.current_timestamp(),
                },
            )
            await session.execute(stmt)
            await session.commit()

    async def get_speculative_start_ids(self) -> dict[str, int]:
        """Get all speculative starting IDs.

        Returns:
            Dict mapping step names to their starting_id.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    SpeculativeStartId.step_name,
                    SpeculativeStartId.starting_id,
                )
            )
            return {row[0]: row[1] for row in result.all()}

    async def clear_speculative_start_id(self, step_name: str) -> None:
        """Clear a speculative starting ID for a step.

        Args:
            step_name: The name of the speculative step method.
        """
        async with self._lock, self._session_factory() as session:
            await session.execute(
                delete(SpeculativeStartId).where(
                    SpeculativeStartId.step_name == step_name
                )
            )
            await session.commit()

    async def clear_all_speculative_start_ids(self) -> None:
        """Clear all speculative starting IDs."""
        async with self._lock, self._session_factory() as session:
            await session.execute(delete(SpeculativeStartId))
            await session.commit()

    # --- Speculation Tracking (new @speculate pattern) ---

    async def save_speculation_state(
        self,
        func_name: str,
        highest_successful_id: int,
        consecutive_failures: int,
        current_ceiling: int,
        stopped: bool,
    ) -> None:
        """Save or update speculation tracking state for a @speculate function.

        Args:
            func_name: Name of the @speculate decorated function.
            highest_successful_id: Highest ID that returned 2xx.
            consecutive_failures: Count of failures beyond highest_successful_id.
            current_ceiling: Current upper bound of seeded IDs.
            stopped: Whether speculation has stopped for this function.
        """
        async with self._lock, self._session_factory() as session:
            stmt = sqlite_insert(SpeculationTracking).values(
                func_name=func_name,
                highest_successful_id=highest_successful_id,
                consecutive_failures=consecutive_failures,
                current_ceiling=current_ceiling,
                stopped=stopped,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["func_name"],
                set_={
                    "highest_successful_id": stmt.excluded.highest_successful_id,
                    "consecutive_failures": stmt.excluded.consecutive_failures,
                    "current_ceiling": stmt.excluded.current_ceiling,
                    "stopped": stmt.excluded.stopped,
                    "updated_at": func.current_timestamp(),
                },
            )
            await session.execute(stmt)
            await session.commit()

    async def load_speculation_state(
        self, func_name: str
    ) -> dict[str, int | bool] | None:
        """Load speculation tracking state for a @speculate function.

        Args:
            func_name: Name of the @speculate decorated function.

        Returns:
            Dict with keys: highest_successful_id, consecutive_failures,
            current_ceiling, stopped. Returns None if no state exists.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(SpeculationTracking).where(
                    SpeculationTracking.func_name == func_name
                )
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return {
                "func_name": row.func_name,
                "highest_successful_id": row.highest_successful_id,
                "consecutive_failures": row.consecutive_failures,
                "current_ceiling": row.current_ceiling,
                "stopped": bool(row.stopped),
            }

    async def load_all_speculation_states(
        self,
    ) -> dict[str, dict[str, int | bool]]:
        """Load all speculation tracking states.

        Returns:
            Dict mapping func_name to their state dict.
        """
        async with self._session_factory() as session:
            result = await session.execute(select(SpeculationTracking))
            rows = result.scalars().all()
            return {
                row.func_name: {
                    "highest_successful_id": row.highest_successful_id,
                    "consecutive_failures": row.consecutive_failures,
                    "current_ceiling": row.current_ceiling,
                    "stopped": bool(row.stopped),
                }
                for row in rows
            }

    async def get_all_speculation_progress(
        self,
    ) -> dict[str, int]:
        """Get highest_successful_id for all speculation tracking entries.

        Returns:
            Dict mapping func_name to their highest_successful_id.
        """
        states = await self.load_all_speculation_states()
        return {
            func_name: state["highest_successful_id"]
            for func_name, state in states.items()
        }

    async def clear_speculation_state(self, func_name: str) -> None:
        """Clear speculation tracking state for a @speculate function.

        Args:
            func_name: Name of the @speculate decorated function.
        """
        async with self._lock, self._session_factory() as session:
            await session.execute(
                delete(SpeculationTracking).where(
                    SpeculationTracking.func_name == func_name
                )
            )
            await session.commit()

    async def clear_all_speculation_states(self) -> None:
        """Clear all speculation tracking states."""
        async with self._lock, self._session_factory() as session:
            await session.execute(delete(SpeculationTracking))
            await session.commit()
