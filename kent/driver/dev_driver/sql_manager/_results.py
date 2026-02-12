"""Result storage operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING

from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.dev_driver.models import Result

if TYPE_CHECKING:
    import asyncio


class ResultStorageMixin:
    """Result table database operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    async def store_result(
        self,
        request_id: int,
        result_type: str,
        data_json: str,
        is_valid: bool = True,
        validation_errors_json: str | None = None,
    ) -> int:
        """Store a scraped result.

        Args:
            request_id: The database ID of the request that produced this.
            result_type: Pydantic model class name.
            data_json: JSON-encoded result data.
            is_valid: Whether the data passed validation.
            validation_errors_json: JSON-encoded validation errors if invalid.

        Returns:
            The database ID of the stored result.
        """
        async with self._lock, self._session_factory() as session:
            res = Result(
                request_id=request_id,
                result_type=result_type,
                data_json=data_json,
                is_valid=is_valid,
                validation_errors_json=validation_errors_json,
            )
            session.add(res)
            await session.commit()
            await session.refresh(res)
            return res.id  # type: ignore[return-value]
