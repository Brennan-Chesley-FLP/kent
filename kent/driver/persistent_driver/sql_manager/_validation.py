"""Response validation operations for SQLManager."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from kent.driver.persistent_driver.models import Request

if TYPE_CHECKING:
    import asyncio


class ValidationMixin:
    """JSON and XML response validation operations."""

    _lock: asyncio.Lock
    _session_factory: async_sessionmaker

    # --- JSON Response Validation ---

    async def validate_json_responses(
        self,
        continuation: str,
        model: type[BaseModel],
    ) -> list[int]:
        """Validate stored JSON responses against a Pydantic model.

        Args:
            continuation: The continuation method name to filter responses.
            model: Pydantic BaseModel class to validate against.

        Returns:
            List of request_id values for responses that failed validation.
        """
        from kent.driver.persistent_driver.compression import (
            decompress_response,
        )

        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.id,
                    Request.content_compressed,
                    Request.compression_dict_id,
                ).where(
                    Request.continuation == continuation,
                    Request.response_status_code.isnot(None),  # type: ignore[union-attr]
                )
            )
            rows = result.all()

        if not rows:
            return []

        invalid_request_ids = []

        for row in rows:
            request_id, compressed_content, dict_id = row

            if compressed_content is None:
                continue

            try:
                content = await decompress_response(
                    self._session_factory,
                    compressed_content,
                    dict_id,
                )
                content_str = content.decode("utf-8")
                data = json.loads(content_str)
                model.model_validate(data)
            except Exception:
                invalid_request_ids.append(request_id)

        return invalid_request_ids

    # --- XML/XSD Response Validation ---

    async def validate_xml_responses(
        self,
        continuation: str,
        xsd_path: str,
    ) -> list[int]:
        """Validate stored HTML responses against an XSD schema.

        Args:
            continuation: The continuation method name to filter responses.
            xsd_path: Absolute path to the XSD schema file.

        Returns:
            List of request_id values for responses that failed validation.
        """
        from lxml import etree
        from lxml import html as lxml_html

        from kent.driver.persistent_driver.compression import (
            decompress_response,
        )

        schema_doc = etree.parse(xsd_path)  # noqa: S320
        schema = etree.XMLSchema(schema_doc)

        async with self._session_factory() as session:
            result = await session.execute(
                select(
                    Request.id,
                    Request.content_compressed,
                    Request.compression_dict_id,
                ).where(
                    Request.continuation == continuation,
                    Request.response_status_code.isnot(None),  # type: ignore[union-attr]
                )
            )
            rows = result.all()

        if not rows:
            return []

        invalid_request_ids = []

        for row in rows:
            request_id, compressed_content, dict_id = row

            if compressed_content is None:
                continue

            try:
                content = await decompress_response(
                    self._session_factory,
                    compressed_content,
                    dict_id,
                )
                html_tree = lxml_html.fromstring(content)
                schema.validate(html_tree)
            except Exception:
                invalid_request_ids.append(request_id)

        return invalid_request_ids
