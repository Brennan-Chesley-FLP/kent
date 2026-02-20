"""WARC export functionality for LocalDevDriver.

This module provides functionality to export stored responses from the
database to WARC (Web ARChive) format, enabling archival and replay
of HTTP traffic.
"""

from __future__ import annotations

import json
import logging
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

from sqlmodel import select
from warcio.statusandheaders import StatusAndHeaders
from warcio.warcwriter import WARCWriter

from kent.driver.dev_driver.models import (
    IncidentalRequest,
    Request,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import async_sessionmaker

logger = logging.getLogger(__name__)


async def export_warc(
    session_factory: async_sessionmaker,
    output_path: Path,
    compress: bool = True,
    continuation: str | None = None,
) -> int:
    """Export all responses from database to WARC file.

    Iterates through all stored responses, decompresses them, and
    writes them to a WARC file with request/response record pairs.

    Args:
        session_factory: Async session factory.
        output_path: Path for output WARC file. If compress=True and
            path doesn't end with .gz, it will be appended.
        compress: Whether to gzip-compress the WARC file.
        continuation: If specified, only export responses for requests
            with this continuation method.

    Returns:
        Number of responses exported.
    """
    from kent.driver.dev_driver.compression import (
        decompress_response,
    )

    # Ensure output path has correct extension
    if compress and not str(output_path).endswith(".gz"):
        output_path = Path(str(output_path) + ".gz")

    # Create parent directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Build query - all data is now in the requests table
    stmt = (
        select(
            Request.id,
            Request.response_status_code,
            Request.response_headers_json,
            Request.response_url,
            Request.content_compressed,
            Request.compression_dict_id,
            Request.warc_record_id,
            Request.method,
            Request.url,
            Request.headers_json,
            Request.body,
        )
        .where(
            Request.response_status_code.isnot(None),  # type: ignore[union-attr]
        )
        .order_by(Request.id)
    )

    if continuation:
        stmt = stmt.where(Request.continuation == continuation)

    async with session_factory() as session:
        result = await session.execute(stmt)
        rows = result.all()

    if not rows:
        logger.info("No responses to export")
        return 0

    count = 0
    with output_path.open("wb") as f:
        writer = WARCWriter(f, gzip=compress)

        for row in rows:
            (
                request_id,
                status_code,
                response_headers_json,
                response_url,
                content_compressed,
                compression_dict_id,
                warc_record_id,
                method,
                request_url,
                request_headers_json,
                request_body,
            ) = row

            # Decompress content
            if content_compressed:
                try:
                    content = await decompress_response(
                        session_factory,
                        content_compressed,
                        compression_dict_id,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to decompress response for request {request_id}: {e}"
                    )
                    continue
            else:
                content = b""

            # Parse response headers
            response_headers = []
            if response_headers_json:
                headers_dict = json.loads(response_headers_json)
                response_headers = list(headers_dict.items())

            # Build HTTP response headers
            http_headers = StatusAndHeaders(
                statusline=f"{status_code} OK",
                headers=response_headers,
                protocol="HTTP/1.1",
            )

            # Create response record
            payload_stream = BytesIO(content)
            response_record = writer.create_warc_record(
                uri=response_url,
                record_type="response",
                payload=payload_stream,
                http_headers=http_headers,
                warc_headers_dict={
                    "WARC-Record-ID": f"<urn:uuid:{warc_record_id}>",
                    "X-HTTP-Method": method,
                },
            )
            writer.write_record(response_record)

            # Create request record
            request_headers = []
            if request_headers_json:
                req_headers_dict = json.loads(request_headers_json)
                request_headers = list(req_headers_dict.items())

            request_http_headers = StatusAndHeaders(
                statusline=f"{method} {request_url} HTTP/1.1",
                headers=request_headers,
                protocol="HTTP/1.1",
                is_http_request=True,
            )

            request_payload = BytesIO(request_body or b"")
            request_record = writer.create_warc_record(
                uri=request_url,
                record_type="request",
                payload=request_payload,
                http_headers=request_http_headers,
                warc_headers_dict={
                    "WARC-Concurrent-To": f"<urn:uuid:{warc_record_id}>",
                },
            )
            writer.write_record(request_record)

            count += 1
            logger.debug(
                f"Exported response for request {request_id} ({response_url})"
            )

            # Export incidental requests associated with this parent request
            async with session_factory() as session:
                inc_result = await session.execute(
                    select(IncidentalRequest)
                    .where(IncidentalRequest.parent_request_id == request_id)
                    .order_by(IncidentalRequest.started_at_ns.asc())  # type: ignore[union-attr]
                )
                inc_rows = inc_result.scalars().all()

            for inc in inc_rows:
                # Skip failed incidental requests (no response)
                if not inc.status_code:
                    logger.debug(
                        f"Skipping failed incidental request {inc.id} ({inc.url})"
                    )
                    continue

                # Decompress incidental content if present
                inc_content = b""
                if inc.content_compressed:
                    try:
                        inc_content = await decompress_response(
                            session_factory,
                            inc.content_compressed,
                            inc.compression_dict_id,
                        )
                    except Exception as e:
                        logger.warning(
                            f"Failed to decompress incidental request {inc.id}: {e}"
                        )
                        continue

                # Parse incidental response headers
                inc_response_headers = []
                if inc.response_headers_json:
                    inc_headers_dict = json.loads(inc.response_headers_json)
                    inc_response_headers = list(inc_headers_dict.items())

                # Build HTTP response headers for incidental request
                inc_http_headers = StatusAndHeaders(
                    statusline=f"{inc.status_code} OK",
                    headers=inc_response_headers,
                    protocol="HTTP/1.1",
                )

                # Create response record for incidental request
                inc_payload_stream = BytesIO(inc_content)
                inc_response_record = writer.create_warc_record(
                    uri=inc.url,
                    record_type="response",
                    payload=inc_payload_stream,
                    http_headers=inc_http_headers,
                    warc_headers_dict={
                        "X-HTTP-Method": inc.method,
                        "X-Resource-Type": inc.resource_type,
                        "X-From-Cache": str(inc.from_cache),
                        "X-Parent-WARC-Record-ID": f"<urn:uuid:{warc_record_id}>",
                    },
                )
                writer.write_record(inc_response_record)

                # Create request record for incidental request
                inc_request_headers = []
                if inc.headers_json:
                    inc_req_headers_dict = json.loads(inc.headers_json)
                    inc_request_headers = list(inc_req_headers_dict.items())

                inc_request_http_headers = StatusAndHeaders(
                    statusline=f"{inc.method} {inc.url} HTTP/1.1",
                    headers=inc_request_headers,
                    protocol="HTTP/1.1",
                    is_http_request=True,
                )

                inc_request_payload = BytesIO(inc.body or b"")
                inc_request_record = writer.create_warc_record(
                    uri=inc.url,
                    record_type="request",
                    payload=inc_request_payload,
                    http_headers=inc_request_http_headers,
                    warc_headers_dict={
                        "X-Resource-Type": inc.resource_type,
                        "X-Parent-WARC-Record-ID": f"<urn:uuid:{warc_record_id}>",
                    },
                )
                writer.write_record(inc_request_record)

                logger.debug(
                    f"Exported incidental {inc.resource_type} {inc.id} ({inc.url})"
                )

    logger.info(f"Exported {count} responses to {output_path}")
    return count


async def export_warc_for_continuation(
    session_factory: async_sessionmaker,
    continuation: str,
    output_path: Path,
    compress: bool = True,
) -> int:
    """Export responses for a specific continuation to WARC file.

    Args:
        session_factory: Async session factory.
        continuation: The continuation method name to filter by.
        output_path: Path for output WARC file.
        compress: Whether to gzip-compress the WARC file.

    Returns:
        Number of responses exported.
    """
    from kent.driver.dev_driver.compression import (
        decompress_response,
    )

    # Ensure output path has correct extension
    if compress and not str(output_path).endswith(".gz"):
        output_path = Path(str(output_path) + ".gz")

    # Create parent directory if needed
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Query responses for specific continuation - all in requests table
    stmt = (
        select(
            Request.id,
            Request.response_status_code,
            Request.response_headers_json,
            Request.response_url,
            Request.content_compressed,
            Request.compression_dict_id,
            Request.warc_record_id,
            Request.method,
            Request.url,
            Request.headers_json,
            Request.body,
        )
        .where(
            Request.continuation == continuation,
            Request.response_status_code.isnot(None),  # type: ignore[union-attr]
        )
        .order_by(Request.id)
    )

    async with session_factory() as session:
        result = await session.execute(stmt)
        rows = result.all()

    if not rows:
        logger.info(f"No responses for continuation '{continuation}'")
        return 0

    count = 0
    with output_path.open("wb") as f:
        writer = WARCWriter(f, gzip=compress)

        for row in rows:
            (
                request_id,
                status_code,
                response_headers_json,
                response_url,
                content_compressed,
                compression_dict_id,
                warc_record_id,
                method,
                request_url,
                request_headers_json,
                request_body,
            ) = row

            # Decompress content
            if content_compressed:
                try:
                    content = await decompress_response(
                        session_factory,
                        content_compressed,
                        compression_dict_id,
                    )
                except Exception as e:
                    logger.warning(
                        f"Failed to decompress response for request {request_id}: {e}"
                    )
                    continue
            else:
                content = b""

            # Parse response headers
            response_headers = []
            if response_headers_json:
                headers_dict = json.loads(response_headers_json)
                response_headers = list(headers_dict.items())

            # Build HTTP response headers
            http_headers = StatusAndHeaders(
                statusline=f"{status_code} OK",
                headers=response_headers,
                protocol="HTTP/1.1",
            )

            # Create response record
            payload_stream = BytesIO(content)
            response_record = writer.create_warc_record(
                uri=response_url,
                record_type="response",
                payload=payload_stream,
                http_headers=http_headers,
                warc_headers_dict={
                    "WARC-Record-ID": f"<urn:uuid:{warc_record_id}>",
                    "X-HTTP-Method": method,
                },
            )
            writer.write_record(response_record)
            count += 1

    logger.info(f"Exported {count} responses to {output_path}")
    return count
