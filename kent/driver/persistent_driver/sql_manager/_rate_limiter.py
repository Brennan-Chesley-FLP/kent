"""Rate limiter state operations for SQLManager."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from sqlalchemy import func, select, update
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.persistent_driver.models import RateLimiterState

if TYPE_CHECKING:
    import asyncio


class RateLimiterMixin:
    """RateLimiterState table database operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    async def get_rate_limiter_state(
        self,
    ) -> dict[str, Any] | None:
        """Get the current rate limiter state.

        Returns:
            Dictionary with rate limiter state, or None if not initialized.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(RateLimiterState).where(RateLimiterState.id == 1)
            )
            row = result.scalar_one_or_none()
            if row is None:
                return None
            return {
                "tokens": row.tokens,
                "rate": row.rate,
                "bucket_size": row.bucket_size,
                "last_congestion_rate": row.last_congestion_rate,
                "jitter": row.jitter,
                "last_used_at": row.last_used_at,
                "total_requests": row.total_requests,
                "total_successes": row.total_successes,
                "total_rate_limited": row.total_rate_limited,
                "created_at": row.created_at,
                "updated_at": row.updated_at,
            }

    async def upsert_rate_limiter_state(
        self,
        tokens: float,
        rate: float,
        bucket_size: float,
        last_congestion_rate: float,
        jitter: float,
        last_used_at: float,
        total_requests: int = 0,
        total_successes: int = 0,
        total_rate_limited: int = 0,
    ) -> None:
        """Create or update the rate limiter state.

        Args:
            tokens: Current token count.
            rate: Current rate (tokens per second).
            bucket_size: Maximum tokens.
            last_congestion_rate: Rate at last congestion event.
            jitter: Uniform jitter +/-seconds.
            last_used_at: Unix timestamp of last token acquisition.
            total_requests: Total requests made.
            total_successes: Total successful requests.
            total_rate_limited: Total rate-limited requests.
        """
        async with self._lock, self._session_factory() as session:
            stmt = sqlite_insert(RateLimiterState).values(
                id=1,
                tokens=tokens,
                rate=rate,
                bucket_size=bucket_size,
                last_congestion_rate=last_congestion_rate,
                jitter=jitter,
                last_used_at=last_used_at,
                total_requests=total_requests,
                total_successes=total_successes,
                total_rate_limited=total_rate_limited,
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=["id"],
                set_={
                    "tokens": stmt.excluded.tokens,
                    "rate": stmt.excluded.rate,
                    "bucket_size": stmt.excluded.bucket_size,
                    "last_congestion_rate": stmt.excluded.last_congestion_rate,
                    "jitter": stmt.excluded.jitter,
                    "last_used_at": stmt.excluded.last_used_at,
                    "total_requests": stmt.excluded.total_requests,
                    "total_successes": stmt.excluded.total_successes,
                    "total_rate_limited": stmt.excluded.total_rate_limited,
                    "updated_at": func.current_timestamp(),
                },
            )
            await session.execute(stmt)
            await session.commit()

    async def update_rate_limiter_tokens(
        self, tokens: float, last_used_at: float
    ) -> None:
        """Update just the tokens and last_used_at.

        Args:
            tokens: New token count.
            last_used_at: Unix timestamp of token acquisition.
        """
        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(RateLimiterState)
                .where(RateLimiterState.id == 1)
                .values(
                    tokens=tokens,
                    last_used_at=last_used_at,
                    updated_at=func.current_timestamp(),
                )
            )
            await session.commit()

    async def update_rate_limiter_rate_increase(self, new_rate: float) -> None:
        """Update rate after a successful request (rate increase).

        Args:
            new_rate: The new rate after increase.
        """
        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(RateLimiterState)
                .where(RateLimiterState.id == 1)
                .values(
                    rate=new_rate,
                    total_requests=RateLimiterState.total_requests + 1,
                    total_successes=RateLimiterState.total_successes + 1,
                    updated_at=func.current_timestamp(),
                )
            )
            await session.commit()

    async def update_rate_limiter_rate_decrease(
        self, new_rate: float, congestion_rate: float
    ) -> None:
        """Update rate after a rate-limited response (rate decrease).

        Args:
            new_rate: The new rate after decrease.
            congestion_rate: The rate at which congestion occurred.
        """
        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(RateLimiterState)
                .where(RateLimiterState.id == 1)
                .values(
                    rate=new_rate,
                    last_congestion_rate=congestion_rate,
                    tokens=0,
                    total_requests=RateLimiterState.total_requests + 1,
                    total_rate_limited=RateLimiterState.total_rate_limited + 1,
                    updated_at=func.current_timestamp(),
                )
            )
            await session.commit()

    async def increment_rate_limiter_success(self) -> None:
        """Increment success counter without changing rate."""
        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(RateLimiterState)
                .where(RateLimiterState.id == 1)
                .values(
                    total_requests=RateLimiterState.total_requests + 1,
                    total_successes=RateLimiterState.total_successes + 1,
                    updated_at=func.current_timestamp(),
                )
            )
            await session.commit()

    async def increment_rate_limiter_rate_limited(self) -> None:
        """Increment rate-limited counter without changing rate."""
        async with self._lock, self._session_factory() as session:
            await session.execute(
                update(RateLimiterState)
                .where(RateLimiterState.id == 1)
                .values(
                    total_requests=RateLimiterState.total_requests + 1,
                    total_rate_limited=RateLimiterState.total_rate_limited + 1,
                    updated_at=func.current_timestamp(),
                )
            )
            await session.commit()
