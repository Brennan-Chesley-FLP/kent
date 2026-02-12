"""Comparison methods for LocalDevDriverDebugger."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import sqlalchemy as sa
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlmodel import select

from kent.driver.dev_driver.models import (
    Request,
    Response,
)
from kent.driver.dev_driver.sql_manager import (
    Page,
    RequestRecord,
    ResultRecord,
    SQLManager,
)


class ComparisonMixin:
    """Comparison and dry-run methods for scraper output analysis."""

    sql: SQLManager
    _session_factory: async_sessionmaker

    if TYPE_CHECKING:
        # Provided by InspectionMixin at runtime via multiple inheritance.
        async def get_response_content(
            self, response_id: int
        ) -> bytes | None: ...
        async def list_errors(
            self,
            error_type: str | None = None,
            is_resolved: bool | None = None,
            continuation: str | None = None,
            limit: int = 100,
            offset: int = 0,
        ) -> Page[dict[str, Any]]: ...

    async def get_child_requests_transitive(
        self, parent_request_id: int
    ) -> list[RequestRecord]:
        """Get all child requests transitively by parent_request_id.

        Recursively fetches all requests that were generated as children
        of the given parent request, including grandchildren and beyond.

        Args:
            parent_request_id: The parent request ID.

        Returns:
            List of RequestRecord objects for all transitive children.
        """
        # Build a recursive CTE
        base = (
            select(
                Request.id,
                Request.status,
                Request.priority,
                Request.queue_counter,
                Request.method,
                Request.url,
                Request.continuation,
                Request.current_location,
                Request.created_at,
                Request.started_at,
                Request.completed_at,
                Request.retry_count,
                Request.cumulative_backoff,
                Request.last_error,
                Request.created_at_ns,
                Request.started_at_ns,
                Request.completed_at_ns,
            )
            .where(Request.parent_request_id == parent_request_id)
            .cte(name="children", recursive=True)
        )

        req_alias = Request.__table__.alias("r")
        recursive = select(
            req_alias.c.id,
            req_alias.c.status,
            req_alias.c.priority,
            req_alias.c.queue_counter,
            req_alias.c.method,
            req_alias.c.url,
            req_alias.c.continuation,
            req_alias.c.current_location,
            req_alias.c.created_at,
            req_alias.c.started_at,
            req_alias.c.completed_at,
            req_alias.c.retry_count,
            req_alias.c.cumulative_backoff,
            req_alias.c.last_error,
            req_alias.c.created_at_ns,
            req_alias.c.started_at_ns,
            req_alias.c.completed_at_ns,
        ).where(req_alias.c.parent_request_id == base.c.id)

        children_cte = base.union_all(recursive)

        final_query = select(children_cte).order_by(children_cte.c.id)

        async with self._session_factory() as session:
            result = await session.execute(final_query)
            rows = result.all()

        requests = []
        for row in rows:
            requests.append(
                RequestRecord(
                    id=row[0],
                    status=row[1],
                    priority=row[2],
                    queue_counter=row[3],
                    method=row[4],
                    url=row[5],
                    continuation=row[6],
                    current_location=row[7],
                    created_at=row[8],
                    started_at=row[9],
                    completed_at=row[10],
                    retry_count=row[11],
                    cumulative_backoff=row[12],
                    last_error=row[13],
                    created_at_ns=row[14],
                    started_at_ns=row[15],
                    completed_at_ns=row[16],
                )
            )

        return requests

    async def get_results_for_request(
        self, request_id: int
    ) -> list[ResultRecord]:
        """Get all results (ParsedData) for a request.

        Args:
            request_id: The request ID.

        Returns:
            List of ResultRecord objects.
        """
        page = await self.sql.list_results(
            request_id=request_id, limit=10000, offset=0
        )
        return page.items

    async def sample_terminal_requests(
        self, continuation: str, sample_count: int
    ) -> list[int]:
        """Sample terminal requests (requests that produced no child requests).

        Args:
            continuation: The continuation (step name) to sample from.
            sample_count: Number of terminal requests to sample.

        Returns:
            List of request IDs for sampled terminal requests.
        """
        child_alias = Request.__table__.alias("child")
        child_exists = (
            select(sa.literal(1))
            .select_from(child_alias)
            .where(child_alias.c.parent_request_id == Request.id)
            .correlate(Request)
        )

        async with self._session_factory() as session:
            result = await session.execute(
                select(Request.id)
                .where(
                    Request.continuation == continuation,
                    Request.status == "completed",
                    ~sa.exists(child_exists),
                )
                .order_by(sa.func.random())
                .limit(sample_count)
            )
            rows = result.all()

        return [row[0] for row in rows]

    async def sample_requests(
        self, continuation: str, sample_count: int
    ) -> list[int]:
        """Sample completed requests for a continuation (including non-terminal).

        Args:
            continuation: The continuation (step name) to sample from.
            sample_count: Number of requests to sample.

        Returns:
            List of request IDs for sampled requests.
        """
        async with self._session_factory() as session:
            result = await session.execute(
                select(Request.id)
                .where(
                    Request.continuation == continuation,
                    Request.status == "completed",
                )
                .order_by(sa.func.random())
                .limit(sample_count)
            )
            rows = result.all()

        return [row[0] for row in rows]

    async def compare_continuation(
        self,
        request_id: int,
        scraper_class: type,
    ) -> Any:
        """Compare continuation output between stored and dry-run execution.

        Args:
            request_id: The request ID to compare.
            scraper_class: The scraper class to instantiate for dry-run.

        Returns:
            ComparisonResult with detailed diffs.

        Raises:
            ValueError: If request not found or no response available.
        """
        from kent.driver.dev_driver.comparison import (
            ComparisonResult,
            compare_continuation_output,
        )
        from kent.driver.dev_driver.dry_run_driver import (
            DryRunDriver,
            DryRunResult,
        )

        # Get the full request data
        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.id,
                    Request.url,
                    Request.method,
                    Request.continuation,
                    Request.current_location,
                    Request.accumulated_data_json,
                    Request.aux_data_json,
                    Request.permanent_json,
                ).where(Request.id == request_id)
            )
            request_row = result.first()
            if not request_row:
                raise ValueError(f"Request {request_id} not found")

        request_data = {
            "url": request_row[1],
            "method": request_row[2],
            "continuation": request_row[3],
            "current_location": request_row[4],
            "accumulated_data_json": request_row[5],
            "aux_data_json": request_row[6],
            "permanent_json": request_row[7],
        }

        # Get the response for this request
        async with self._session_factory() as session:
            result = await session.execute(
                select(Response)
                .where(Response.request_id == request_id)
                .limit(1)
            )
            response_obj = result.scalars().first()
            if not response_obj:
                raise ValueError(f"No response found for request {request_id}")

        response_data = {
            "id": response_obj.id,
            "request_id": response_obj.request_id,
            "status_code": response_obj.status_code,
            "headers_json": response_obj.headers_json,
            "url": response_obj.url,
            "content_compressed": response_obj.content_compressed,
            "content_size_original": response_obj.content_size_original,
            "content_size_compressed": response_obj.content_size_compressed,
            "compression_dict_id": response_obj.compression_dict_id,
            "continuation": response_obj.continuation,
            "created_at": response_obj.created_at,
            "warc_record_id": response_obj.warc_record_id,
            "speculation_outcome": response_obj.speculation_outcome,
        }

        # Get decompressed content
        response_content = await self.get_response_content(response_data["id"])
        if response_content is None:
            raise ValueError(
                f"No content available for response {response_data['id']}"
            )

        response_data["content"] = response_content
        try:
            response_data["text"] = response_content.decode("utf-8")
        except UnicodeDecodeError:
            response_data["text"] = ""

        # Load original stored results using recursive CTE
        base_children = (
            select(
                Request.id,
                Request.request_type,
                Request.url,
                Request.method,
                Request.continuation,
                Request.current_location,
                Request.accumulated_data_json,
                Request.aux_data_json,
                Request.permanent_json,
                Request.priority,
                Request.deduplication_key,
                Request.expected_type,
            )
            .where(Request.parent_request_id == request_id)
            .cte(name="children", recursive=True)
        )

        req_alias = Request.__table__.alias("r")
        recursive_children = select(
            req_alias.c.id,
            req_alias.c.request_type,
            req_alias.c.url,
            req_alias.c.method,
            req_alias.c.continuation,
            req_alias.c.current_location,
            req_alias.c.accumulated_data_json,
            req_alias.c.aux_data_json,
            req_alias.c.permanent_json,
            req_alias.c.priority,
            req_alias.c.deduplication_key,
            req_alias.c.expected_type,
        ).where(req_alias.c.parent_request_id == base_children.c.id)

        children_cte = base_children.union_all(recursive_children)

        async with self._session_factory() as session:
            child_result = await session.execute(
                select(children_cte).order_by(children_cte.c.id)
            )
            child_rows = child_result.all()

        original_results = await self.get_results_for_request(request_id)

        from kent.driver.dev_driver.dry_run_driver import (
            CapturedData,
            CapturedRequest,
        )

        original_requests = []
        for row in child_rows:
            original_requests.append(
                CapturedRequest(
                    request_type=row[1] or "navigating",
                    url=row[2],
                    method=row[3],
                    continuation=row[4],
                    accumulated_data=(json.loads(row[6]) if row[6] else {}),
                    aux_data=(json.loads(row[7]) if row[7] else {}),
                    permanent=(json.loads(row[8]) if row[8] else {}),
                    current_location=row[5] or "",
                    priority=row[9],
                    deduplication_key=row[10],
                    is_speculative=False,
                    speculation_id=None,
                    expected_type=row[11],
                )
            )

        original_data = [
            CapturedData(
                data=(json.loads(result.data_json) if result.data_json else {})
            )
            for result in original_results
        ]

        original: DryRunResult = DryRunResult(
            requests=original_requests, data=original_data, error=None
        )

        # Check if there was an error for this request
        errors_page = await self.list_errors(
            continuation=request_data["continuation"],
            is_resolved=None,
            limit=1000,
            offset=0,
        )
        original_error = None
        for error in errors_page.items:
            if error["request_id"] == request_id and not error["is_resolved"]:
                from kent.driver.dev_driver.dry_run_driver import (
                    CapturedError,
                )

                original_error = CapturedError(
                    error_type=error["error_type"],
                    error_message=error["message"],
                )
                break

        original.error = original_error

        # Run dry-run with new code
        scraper_instance = scraper_class()
        driver = DryRunDriver(scraper_instance)
        new = driver.run_continuation(
            request_data["continuation"], response_data, request_data
        )

        # Compare
        comparison_result: ComparisonResult = compare_continuation_output(
            request_id=request_id,
            request_url=request_data["url"],
            continuation=request_data["continuation"],
            original=original,
            new=new,
        )

        return comparison_result

    async def compare_request_tree(
        self,
        request_id: int,
        scraper_class: type,
    ) -> list[Any]:
        """Compare entire request tree starting from a request.

        Args:
            request_id: The root request ID to start comparison from.
            scraper_class: The scraper class to instantiate for dry-run.

        Returns:
            List of ComparisonResult for each request in the tree.
        """
        from collections import deque

        results = []
        queue: deque[int] = deque([request_id])
        visited: set[int] = set()

        while queue:
            current_id = queue.popleft()
            if current_id in visited:
                continue
            visited.add(current_id)

            try:
                result = await self.compare_continuation(
                    current_id, scraper_class
                )
                results.append(result)

                async with self._session_factory() as session:
                    child_result = await session.execute(
                        select(Request.id).where(
                            Request.parent_request_id == current_id,
                            Request.status == "completed",
                        )
                    )
                    child_rows = child_result.all()

                for row in child_rows:
                    child_id = row[0]
                    if child_id not in visited:
                        queue.append(child_id)

            except ValueError:
                pass

        return results
