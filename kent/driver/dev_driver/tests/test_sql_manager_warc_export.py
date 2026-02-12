"""Tests for WARC export functionality (warc_export.py)."""

from __future__ import annotations

import json
import uuid
from pathlib import Path

from kent.driver.dev_driver.compression import compress
from kent.driver.dev_driver.sql_manager import SQLManager


class TestWarcExport:
    """Tests for WARC export functionality via warc_export module."""

    async def test_export_warc_basic(
        self, sql_manager: SQLManager, tmp_path: Path
    ) -> None:
        """Test basic WARC export."""
        from kent.driver.dev_driver.warc_export import (
            export_warc,
        )

        # Create a request and response
        request_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="GET",
            url="https://example.com/page",
            headers_json=json.dumps({"Accept": "text/html"}),
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )
        await sql_manager.mark_request_completed(request_id)

        content = b"<html>Test page</html>"
        compressed = compress(content)
        warc_id = str(uuid.uuid4())

        await sql_manager.store_response(
            request_id=request_id,
            status_code=200,
            headers_json=json.dumps({"Content-Type": "text/html"}),
            url="https://example.com/page",
            compressed_content=compressed,
            content_size_original=len(content),
            content_size_compressed=len(compressed),
            dict_id=None,
            continuation="parse",
            warc_record_id=warc_id,
        )

        output_path = tmp_path / "test.warc.gz"
        count = await export_warc(sql_manager._session_factory, output_path)

        assert count == 1
        assert output_path.exists()

    async def test_export_warc_by_continuation(
        self, sql_manager: SQLManager, tmp_path: Path
    ) -> None:
        """Test WARC export filtered by continuation."""
        from kent.driver.dev_driver.warc_export import (
            export_warc,
        )

        # Create requests with different continuations
        for cont, url in [
            ("parse_listing", "https://example.com/listing1"),
            ("parse_listing", "https://example.com/listing2"),
            ("parse_detail", "https://example.com/detail1"),
        ]:
            request_id = await sql_manager.insert_request(
                priority=5,
                request_type="navigating",
                method="GET",
                url=url,
                headers_json=None,
                cookies_json=None,
                body=None,
                continuation=cont,
                current_location="",
                accumulated_data_json=None,
                aux_data_json=None,
                permanent_json=None,
                expected_type=None,
                dedup_key=url,
                parent_id=None,
            )
            await sql_manager.mark_request_completed(request_id)

            content = f"<html>Content for {url}</html>".encode()
            compressed = compress(content)

            await sql_manager.store_response(
                request_id=request_id,
                status_code=200,
                headers_json=None,
                url=url,
                compressed_content=compressed,
                content_size_original=len(content),
                content_size_compressed=len(compressed),
                dict_id=None,
                continuation=cont,
                warc_record_id=str(uuid.uuid4()),
            )

        # Export only parse_listing
        listing_path = tmp_path / "listing.warc.gz"
        count = await export_warc(
            sql_manager._session_factory,
            listing_path,
            continuation="parse_listing",
        )
        assert count == 2

        # Export only parse_detail
        detail_path = tmp_path / "detail.warc.gz"
        count = await export_warc(
            sql_manager._session_factory,
            detail_path,
            continuation="parse_detail",
        )
        assert count == 1

        # Export all
        all_path = tmp_path / "all.warc.gz"
        count = await export_warc(sql_manager._session_factory, all_path)
        assert count == 3

    async def test_export_warc_empty(
        self, sql_manager: SQLManager, tmp_path: Path
    ) -> None:
        """Test WARC export with no responses."""
        from kent.driver.dev_driver.warc_export import (
            export_warc,
        )

        output_path = tmp_path / "empty.warc"
        count = await export_warc(
            sql_manager._session_factory,
            output_path,
            continuation="nonexistent",
        )

        assert count == 0

    async def test_export_warc_headers_only(
        self, sql_manager: SQLManager, tmp_path: Path
    ) -> None:
        """Test WARC export for headers-only response."""
        from kent.driver.dev_driver.warc_export import (
            export_warc,
        )

        request_id = await sql_manager.insert_request(
            priority=5,
            request_type="navigating",
            method="HEAD",
            url="https://example.com/resource",
            headers_json=None,
            cookies_json=None,
            body=None,
            continuation="parse",
            current_location="",
            accumulated_data_json=None,
            aux_data_json=None,
            permanent_json=None,
            expected_type=None,
            dedup_key=None,
            parent_id=None,
        )
        await sql_manager.mark_request_completed(request_id)

        await sql_manager.store_response(
            request_id=request_id,
            status_code=200,
            headers_json=json.dumps(
                {"Content-Type": "application/pdf", "Content-Length": "5000"}
            ),
            url="https://example.com/resource",
            compressed_content=None,
            content_size_original=0,
            content_size_compressed=0,
            dict_id=None,
            continuation="parse",
            warc_record_id=str(uuid.uuid4()),
        )

        output_path = tmp_path / "headers_only.warc.gz"
        count = await export_warc(sql_manager._session_factory, output_path)

        assert count == 1
        assert output_path.exists()
