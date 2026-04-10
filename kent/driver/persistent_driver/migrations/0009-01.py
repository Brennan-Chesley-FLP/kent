"""Baseline schema — applied by SQLModel.metadata.create_all()."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncEngine


async def migrate(engine: AsyncEngine) -> bool:
    return True
