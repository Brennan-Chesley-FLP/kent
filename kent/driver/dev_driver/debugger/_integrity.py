"""Integrity check methods for LocalDevDriverDebugger."""

from __future__ import annotations

from typing import Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import select

from kent.driver.dev_driver.models import (
    Request,
    Response,
    Result,
)


class IntegrityMixin:
    """Integrity checks: orphaned requests/responses, ghost requests."""

    _session_factory: async_sessionmaker

    async def check_integrity(self) -> dict[str, Any]:
        """Check database integrity for orphaned requests and responses.

        Detects two types of integrity issues:
        1. Orphaned requests: completed requests with no corresponding response
        2. Orphaned responses: responses with no matching request

        Returns:
            Dictionary with integrity check results:
                - orphaned_requests: {count: int, ids: list[int]}
                - orphaned_responses: {count: int, ids: list[int]}
                - has_issues: bool (True if any orphans found)
        """
        async with self._session_factory() as session:
            # Orphaned requests: completed requests with no response
            orphaned_req_stmt = (
                select(Request.id)
                .outerjoin(Response, Request.id == Response.request_id)
                .where(Request.status == "completed", Response.id.is_(None))  # type: ignore[union-attr]
                .order_by(Request.id)
            )
            orphaned_req_count_stmt = (
                select(sa.func.count())
                .select_from(Request)
                .outerjoin(Response, Request.id == Response.request_id)
                .where(Request.status == "completed", Response.id.is_(None))  # type: ignore[union-attr]
            )

            count_result = await session.execute(orphaned_req_count_stmt)
            orphaned_requests_count = count_result.scalar() or 0

            ids_result = await session.execute(orphaned_req_stmt)
            orphaned_request_ids = [row[0] for row in ids_result.all()]

            # Orphaned responses: responses with no matching request
            orphaned_resp_stmt = (
                select(Response.id)
                .outerjoin(Request, Response.request_id == Request.id)
                .where(Request.id.is_(None))  # type: ignore[union-attr]
                .order_by(Response.id)
            )
            orphaned_resp_count_stmt = (
                select(sa.func.count())
                .select_from(Response)
                .outerjoin(Request, Response.request_id == Request.id)
                .where(Request.id.is_(None))  # type: ignore[union-attr]
            )

            count_result = await session.execute(orphaned_resp_count_stmt)
            orphaned_responses_count = count_result.scalar() or 0

            ids_result = await session.execute(orphaned_resp_stmt)
            orphaned_response_ids = [row[0] for row in ids_result.all()]

        has_issues = (
            orphaned_requests_count > 0 or orphaned_responses_count > 0
        )

        return {
            "orphaned_requests": {
                "count": orphaned_requests_count,
                "ids": orphaned_request_ids,
            },
            "orphaned_responses": {
                "count": orphaned_responses_count,
                "ids": orphaned_response_ids,
            },
            "has_issues": has_issues,
        }

    async def get_orphan_details(self) -> dict[str, Any]:
        """Get detailed information about orphaned requests and responses.

        Returns full details for each orphaned request and response, unlike
        check_integrity() which only returns counts and IDs.

        Returns:
            Dictionary with detailed orphan information:
                - orphaned_requests: List of dicts with {id, url, continuation, completed_at}
                - orphaned_responses: List of dicts with {id, request_id, url, created_at}
        """
        async with self._session_factory() as session:
            # Get orphaned request details
            orphaned_req_result = await session.execute(
                select(
                    Request.id,
                    Request.url,
                    Request.continuation,
                    Request.completed_at,
                )
                .outerjoin(Response, Request.id == Response.request_id)
                .where(Request.status == "completed", Response.id.is_(None))  # type: ignore[union-attr]
                .order_by(Request.id)
            )
            orphaned_requests = [
                {
                    "id": row[0],
                    "url": row[1],
                    "continuation": row[2],
                    "completed_at": row[3],
                }
                for row in orphaned_req_result.all()
            ]

            # Get orphaned response details
            orphaned_resp_result = await session.execute(
                select(
                    Response.id,
                    Response.request_id,
                    Response.url,
                    Response.created_at,
                )
                .outerjoin(Request, Response.request_id == Request.id)
                .where(Request.id.is_(None))  # type: ignore[union-attr]
                .order_by(Response.id)
            )
            orphaned_responses = [
                {
                    "id": row[0],
                    "request_id": row[1],
                    "url": row[2],
                    "created_at": row[3],
                }
                for row in orphaned_resp_result.all()
            ]

        return {
            "orphaned_requests": orphaned_requests,
            "orphaned_responses": orphaned_responses,
        }

    async def get_ghost_requests(self) -> dict[str, Any]:
        """Get ghost requests (completed requests with no children and no results).

        Ghost requests are completed requests that produced no observable output:
        no child requests and no ParsedData results.

        Returns:
            Dictionary with ghost request information:
                - total_count: Total number of ghost requests
                - by_continuation: Dict mapping continuation -> count
                - ghosts: List of dicts with {id, url, continuation, completed_at}
        """
        # Subqueries for NOT EXISTS
        child = Request.__table__.alias("child")
        child_exists = (
            select(sa.literal(1))
            .select_from(child)
            .where(child.c.parent_request_id == Request.id)
            .correlate(Request)
        )
        result_exists = (
            select(sa.literal(1))
            .select_from(Result)
            .where(Result.request_id == Request.id)
            .correlate(Request)
        )

        # Base ghost condition
        ghost_conditions = [
            Request.status == "completed",
            ~sa.exists(child_exists),
            ~sa.exists(result_exists),
        ]

        async with self._session_factory() as session:
            # Get total count
            count_stmt = select(sa.func.count()).select_from(Request)
            for cond in ghost_conditions:
                count_stmt = count_stmt.where(cond)
            count_result = await session.execute(count_stmt)
            total_count = count_result.scalar() or 0

            # Get counts by continuation
            by_cont_stmt = (
                select(
                    Request.continuation,
                    sa.func.count().label("ghost_count"),
                )
                .group_by(Request.continuation)
                .order_by(Request.continuation)
            )
            for cond in ghost_conditions:
                by_cont_stmt = by_cont_stmt.where(cond)
            by_cont_result = await session.execute(by_cont_stmt)
            by_continuation: dict[str, int] = {}
            for row in by_cont_result.all():
                by_continuation[row[0]] = row[1]

            # Get detailed ghost request list
            ghost_stmt = select(
                Request.id,
                Request.url,
                Request.continuation,
                Request.completed_at,
            ).order_by(Request.continuation, Request.id)
            for cond in ghost_conditions:
                ghost_stmt = ghost_stmt.where(cond)
            ghost_result = await session.execute(ghost_stmt)
            ghosts = [
                {
                    "id": row[0],
                    "url": row[1],
                    "continuation": row[2],
                    "completed_at": row[3],
                }
                for row in ghost_result.all()
            ]

        return {
            "total_count": total_count,
            "by_continuation": by_continuation,
            "ghosts": ghosts,
        }
