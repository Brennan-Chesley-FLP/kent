"""Tests for internal subsystems: compression, stats, export, rate limiting."""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa


class TestCompression:
    """Tests for compression module."""

    async def test_basic_compress_decompress(self) -> None:
        """Test basic compress/decompress roundtrip."""
        from kent.driver.dev_driver.compression import (
            compress,
            decompress,
        )

        original = b"<html><body>Hello World!</body></html>" * 100
        compressed = compress(original)
        decompressed = decompress(compressed)

        assert decompressed == original
        assert len(compressed) < len(original)

    async def test_compression_ratio(self) -> None:
        """Test that compression achieves good ratios on repetitive content."""
        from kent.driver.dev_driver.compression import (
            compress,
        )

        original = b"<html><body>Test content</body></html>" * 100
        compressed = compress(original)

        ratio = len(original) / len(compressed)
        assert ratio > 10, f"Expected ratio > 10, got {ratio:.2f}"

    async def test_compress_response_no_dict(self, initialized_db) -> None:
        """Test compress_response without dictionary."""
        from kent.driver.dev_driver.compression import (
            compress_response,
            decompress_response,
        )

        engine, session_factory = initialized_db
        content = b"<html>Test</html>"
        compressed, dict_id = await compress_response(
            session_factory, content, "test_continuation"
        )

        assert dict_id is None  # No dictionary available
        assert len(compressed) > 0

        decompressed = await decompress_response(
            session_factory, compressed, dict_id
        )
        assert decompressed == content


class TestStatistics:
    """Tests for statistics module."""

    async def test_queue_stats(self, initialized_db) -> None:
        """Test queue statistics calculation."""
        from kent.driver.dev_driver.stats import (
            get_queue_stats,
        )

        engine, session_factory = initialized_db
        # Create requests with various statuses
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (status, priority, queue_counter, method, url, continuation, current_location)
                VALUES
                ('pending', 9, 1, 'GET', 'https://example.com/1', 'parse', ''),
                ('pending', 9, 2, 'GET', 'https://example.com/2', 'parse', ''),
                ('in_progress', 9, 3, 'GET', 'https://example.com/3', 'parse', ''),
                ('completed', 9, 4, 'GET', 'https://example.com/4', 'parse', ''),
                ('failed', 9, 5, 'GET', 'https://example.com/5', 'process', ''),
                ('held', 9, 6, 'GET', 'https://example.com/6', 'parse', '')
                """)
            )
            await session.commit()

        stats = await get_queue_stats(session_factory)

        assert stats.pending == 2
        assert stats.in_progress == 1
        assert stats.completed == 1
        assert stats.failed == 1
        assert stats.held == 1
        assert stats.total == 6
        assert "parse" in stats.by_continuation
        assert "process" in stats.by_continuation

    async def test_compression_stats(self, initialized_db) -> None:
        """Test compression statistics calculation."""
        from kent.driver.dev_driver.stats import (
            get_compression_stats,
        )

        engine, session_factory = initialized_db
        # Create request first
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url, continuation, current_location)
                VALUES (1, 'completed', 9, 1, 'GET', 'https://example.com', 'parse', '')
                """)
            )

            # Create responses
            await session.execute(
                sa.text("""
                INSERT INTO responses (request_id, status_code, url, content_compressed,
                                       content_size_original, content_size_compressed,
                                       compression_dict_id, continuation, warc_record_id)
                VALUES
                (1, 200, 'https://example.com', x'1234', 1000, 100, NULL, 'parse', 'uuid1')
                """)
            )
            await session.commit()

        stats = await get_compression_stats(session_factory)

        assert stats.total_responses == 1
        assert stats.total_original_bytes == 1000
        assert stats.total_compressed_bytes == 100
        assert stats.compression_ratio == 10.0
        assert stats.no_dict_compressed_count == 1

    async def test_stats_json_serialization(self, initialized_db) -> None:
        """Test that stats can be serialized to JSON."""
        from kent.driver.dev_driver.stats import (
            get_stats,
        )

        engine, session_factory = initialized_db
        # Create run metadata
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO run_metadata (id, scraper_name, status, base_delay, jitter, num_workers, max_backoff_time)
                VALUES (1, 'TestScraper', 'completed', 1.0, 0.5, 1, 60.0)
                """)
            )
            await session.commit()

        stats = await get_stats(session_factory)
        json_str = stats.to_json()

        parsed = json.loads(json_str)
        assert "queue" in parsed
        assert "throughput" in parsed
        assert "compression" in parsed
        assert "results" in parsed
        assert "errors" in parsed
        assert parsed["scraper_name"] == "TestScraper"


class TestWarcExport:
    """Tests for WARC export module."""

    async def test_warc_export(self, initialized_db, tmp_path: Path) -> None:
        """Test exporting responses to WARC file."""
        from kent.driver.dev_driver.compression import (
            compress,
        )
        from kent.driver.dev_driver.warc_export import (
            export_warc,
        )

        engine, session_factory = initialized_db
        # Create request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      headers_json, continuation, current_location)
                VALUES (1, 'completed', 9, 1, 'GET', 'https://example.com/page1',
                        '{"User-Agent": "Test"}', 'parse', '')
                """)
            )

            # Create response with compressed content
            content = b"<html><body>Test page</body></html>"
            compressed = compress(content)

            await session.execute(
                sa.text("""
                INSERT INTO responses (request_id, status_code, headers_json, url,
                                       content_compressed, content_size_original,
                                       content_size_compressed, continuation, warc_record_id)
                VALUES (1, 200, '{"Content-Type": "text/html"}', 'https://example.com/page1',
                        :compressed, :original_size, :compressed_size, 'parse', 'uuid-1')
                """),
                {
                    "compressed": compressed,
                    "original_size": len(content),
                    "compressed_size": len(compressed),
                },
            )
            await session.commit()

        # Export to WARC
        warc_path = tmp_path / "export.warc"
        count = await export_warc(session_factory, warc_path, compress=False)

        assert count == 1
        assert warc_path.exists()

        # Verify WARC content
        from warcio.archiveiterator import ArchiveIterator

        records = []
        with warc_path.open("rb") as f:
            for record in ArchiveIterator(f):
                records.append(record.rec_type)

        # Should have response and request records
        assert "response" in records
        assert "request" in records


class TestDictionaryTraining:
    """Tests for compression dictionary training and recompression."""

    async def test_train_compression_dict(self, initialized_db) -> None:
        """Test training a compression dictionary from stored responses."""
        from kent.driver.dev_driver.compression import (
            compress,
            get_compression_dict,
            train_compression_dict,
        )

        engine, session_factory = initialized_db
        # Create request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      continuation, current_location)
                VALUES (1, 'completed', 9, 1, 'GET', 'https://example.com',
                        'parse', '')
                """)
            )

            # Create multiple responses with similar HTML content (needed for dict training)
            html_template = b"""
            <html>
            <head><title>Court Case {num}</title></head>
            <body>
                <div class="case-header">
                    <h1>Case Number: {num}</h1>
                    <p>Filed: 2024-01-{day:02d}</p>
                </div>
                <div class="case-content">
                    <p>This is the content of case {num}. The parties involved are
                    plaintiff John Doe and defendant Jane Smith. The case concerns
                    a contractual dispute regarding property at 123 Main Street.</p>
                </div>
            </body>
            </html>
            """

            for i in range(20):  # Need enough samples for training
                content = html_template.replace(
                    b"{num}", str(i).encode()
                ).replace(b"{day:02d}", f"{(i % 28) + 1:02d}".encode())
                compressed = compress(content)

                await session.execute(
                    sa.text("""
                    INSERT INTO responses (request_id, status_code, url, content_compressed,
                                           content_size_original, content_size_compressed,
                                           compression_dict_id, continuation, warc_record_id)
                    VALUES (1, 200, :url, :compressed, :original_size, :compressed_size, NULL, 'parse', :warc_id)
                    """),
                    {
                        "url": f"https://example.com/case/{i}",
                        "compressed": compressed,
                        "original_size": len(content),
                        "compressed_size": len(compressed),
                        "warc_id": f"uuid-{i}",
                    },
                )

            await session.commit()

        # Train dictionary
        dict_id = await train_compression_dict(
            session_factory,
            continuation="parse",
            sample_limit=20,
            dict_size=32768,  # Smaller dict for test
        )

        assert dict_id is not None
        assert dict_id > 0

        # Verify dictionary was stored
        result = await get_compression_dict(session_factory, "parse")
        assert result is not None
        stored_id, dict_data = result
        assert stored_id == dict_id
        assert len(dict_data) > 0

    async def test_recompress_responses(self, initialized_db) -> None:
        """Test recompressing responses with a trained dictionary."""
        from kent.driver.dev_driver.compression import (
            compress,
            recompress_responses,
            train_compression_dict,
        )

        engine, session_factory = initialized_db
        # Create request
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (id, status, priority, queue_counter, method, url,
                                      continuation, current_location)
                VALUES (1, 'completed', 9, 1, 'GET', 'https://example.com',
                        'parse', '')
                """)
            )

            # Create responses with similar content
            html_template = b"""
            <html>
            <body>
                <div class="opinion">
                    <h1>Opinion {num}</h1>
                    <p>The court finds that the defendant is liable for damages
                    in the amount of ${amount}. The plaintiff's motion for summary
                    judgment is hereby granted.</p>
                </div>
            </body>
            </html>
            """

            original_sizes = []
            original_compressed_sizes = []

            for i in range(15):
                content = html_template.replace(
                    b"{num}", str(i).encode()
                ).replace(b"{amount}", str(10000 + i * 1000).encode())
                compressed = compress(content)
                original_sizes.append(len(content))
                original_compressed_sizes.append(len(compressed))

                await session.execute(
                    sa.text("""
                    INSERT INTO responses (request_id, status_code, url, content_compressed,
                                           content_size_original, content_size_compressed,
                                           compression_dict_id, continuation, warc_record_id)
                    VALUES (1, 200, :url, :compressed, :original_size, :compressed_size, NULL, 'parse', :warc_id)
                    """),
                    {
                        "url": f"https://example.com/opinion/{i}",
                        "compressed": compressed,
                        "original_size": len(content),
                        "compressed_size": len(compressed),
                        "warc_id": f"uuid-{i}",
                    },
                )

            await session.commit()

        # Train dictionary
        await train_compression_dict(
            session_factory,
            continuation="parse",
            sample_limit=15,
            dict_size=32768,
        )

        # Recompress with dictionary
        count, total_original, total_compressed = await recompress_responses(
            session_factory, "parse"
        )

        assert count == 15
        assert total_original == sum(original_sizes)
        # With dictionary, should achieve better compression
        assert total_compressed < sum(original_compressed_sizes)

        # Verify all responses now have dict_id set
        async with session_factory() as session:
            result = await session.execute(
                sa.text(
                    "SELECT compression_dict_id FROM responses WHERE compression_dict_id IS NOT NULL"
                )
            )
            rows = result.all()
        assert len(rows) == 15

    async def test_train_dict_no_responses_raises(
        self, initialized_db
    ) -> None:
        """Test that training with no responses raises ValueError."""
        from kent.driver.dev_driver.compression import (
            train_compression_dict,
        )

        engine, session_factory = initialized_db
        with pytest.raises(ValueError, match="No responses found"):
            await train_compression_dict(session_factory, "nonexistent")

    async def test_recompress_no_dict_raises(self, initialized_db) -> None:
        """Test that recompressing without a dict raises ValueError."""
        from kent.driver.dev_driver.compression import (
            recompress_responses,
        )

        engine, session_factory = initialized_db
        with pytest.raises(ValueError, match="No dictionary found"):
            await recompress_responses(session_factory, "nonexistent")


class TestCompressionRoundTrip:
    """Tests for compressed response storage and retrieval."""

    async def test_response_compression_roundtrip(self, db_path: Path) -> None:
        """Test that responses are correctly compressed and decompressed."""

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
            Response,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )
        from kent.driver.dev_driver.testing import (
            TestRequestManager,
            create_html_response,
        )

        # Create a large response to ensure compression is used
        large_html = "<html><body>" + ("Content " * 1000) + "</body></html>"

        class SimpleScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com/large",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Response) -> Generator[None, None, None]:
                yield None

        request_manager = TestRequestManager()
        request_manager.add_response(
            "https://example.com/large",
            create_html_response(large_html),
        )

        scraper = SimpleScraper()
        async with LocalDevDriver.open(
            scraper,
            db_path,
            initial_rate=100.0,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            await driver.run()

            # Get the response ID
            async with driver.db._session_factory() as session:
                result = await session.execute(
                    sa.text(
                        "SELECT id, content_size_original, content_size_compressed FROM responses LIMIT 1"
                    )
                )
                row = result.first()

            assert row is not None
            response_id, original_size, compressed_size = row

            # Verify compression happened
            assert original_size > 0
            assert compressed_size > 0
            assert compressed_size < original_size, (
                "Compressed size should be smaller"
            )

            # Retrieve and decompress
            content = await driver.get_response_content(response_id)

            assert content is not None
            assert content.decode("utf-8") == large_html


class TestAioSQLiteBucket:
    """Tests for AioSQLiteBucket rate limiter."""

    async def test_put_and_count(self, initialized_db) -> None:
        """Test adding items and counting."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        rates = [Rate(5, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Initially empty
        count = await bucket.count()
        assert count == 0

        # Add items
        item1 = RateItem(name="test1", timestamp=1000, weight=1)
        item2 = RateItem(name="test2", timestamp=2000, weight=2)

        await bucket.put(item1)
        await bucket.put(item2)

        # Count should be sum of weights
        count = await bucket.count()
        assert count == 3

    async def test_peek(self, initialized_db) -> None:
        """Test peeking at items by index."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        rates = [Rate(5, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Add items with different timestamps
        item1 = RateItem(name="old", timestamp=1000, weight=1)
        item2 = RateItem(name="new", timestamp=2000, weight=1)

        await bucket.put(item1)
        await bucket.put(item2)

        # Peek at index 0 (newest first due to ORDER BY timestamp DESC)
        peeked = await bucket.peek(0)
        assert peeked is not None
        assert peeked.name == "new"
        assert peeked.timestamp == 2000

        # Peek at index 1 (older item)
        peeked = await bucket.peek(1)
        assert peeked is not None
        assert peeked.name == "old"

        # Peek at invalid index
        peeked = await bucket.peek(10)
        assert peeked is None

    async def test_leak(self, initialized_db) -> None:
        """Test leaking expired items."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        # Rate with 1 second interval (1000ms)
        rates = [Rate(5, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Add old and new items
        old_item = RateItem(name="old", timestamp=1000, weight=1)
        new_item = RateItem(name="new", timestamp=5000, weight=1)

        await bucket.put(old_item)
        await bucket.put(new_item)

        # Leak at timestamp 6000 (1 second after new_item)
        # Old item (1000) is older than cutoff (6000 - 1000 = 5000)
        leaked = await bucket.leak(current_timestamp=6000)
        assert leaked == 1

        # Only new item should remain
        count = await bucket.count()
        assert count == 1

    async def test_flush(self, initialized_db) -> None:
        """Test flushing all items."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        rates = [Rate(5, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Add items
        for i in range(5):
            item = RateItem(name=f"test{i}", timestamp=i * 1000, weight=1)
            await bucket.put(item)

        assert await bucket.count() == 5

        # Flush
        await bucket.flush()
        assert await bucket.count() == 0

    async def test_waiting(self, initialized_db) -> None:
        """Test calculating wait time."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        # Rate: 2 requests per second (1000ms)
        rates = [Rate(2, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Add 2 items at timestamp 1000
        item1 = RateItem(name="test1", timestamp=1000, weight=1)
        item2 = RateItem(name="test2", timestamp=1000, weight=1)

        await bucket.put(item1)
        await bucket.put(item2)

        # New item at timestamp 1500 should need to wait
        # The window (1500 - 1000 = 500ms to 1500ms) contains 2 items
        # With limit 2, we're at capacity, so new item needs to wait
        new_item = RateItem(name="new", timestamp=1500, weight=1)
        wait = await bucket.waiting(new_item)

        # Should wait until oldest item (1000) expires (1000 + 1000 = 2000)
        # Wait = 2000 - 1500 = 500ms
        assert wait == 500

    async def test_waiting_no_wait_needed(self, initialized_db) -> None:
        """Test no wait needed when under limit."""
        from pyrate_limiter import Duration, Rate, RateItem

        from kent.driver.dev_driver.rate_limiter import (
            AioSQLiteBucket,
        )

        # Rate: 5 requests per second
        rates = [Rate(5, Duration.SECOND)]
        _, session_factory = initialized_db
        bucket = AioSQLiteBucket(session_factory, rates)

        # Add 1 item
        item = RateItem(name="test", timestamp=1000, weight=1)
        await bucket.put(item)

        # New item at timestamp 1500 should not need to wait (only 1 of 5 used)
        new_item = RateItem(name="new", timestamp=1500, weight=1)
        wait = await bucket.waiting(new_item)

        assert wait == 0


class TestRateLimiterIntegration:
    """Tests for rate limiter integration with LocalDevDriver."""

    async def test_atb_rate_limiter_used_on_init(self, db_path: Path) -> None:
        """Test that ATBAsyncRequestManager is used as the request manager."""
        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.dev_driver.atb_rate_limiter import (
            ATBAsyncRequestManager,
        )
        from kent.driver.dev_driver.dev_driver import (
            LocalDevDriver,
        )

        class MinimalScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Any) -> list:
                return []

        scraper = MinimalScraper()

        async with LocalDevDriver.open(
            scraper, db_path, initial_rate=5.0, enable_monitor=False
        ) as driver:
            # Verify ATBAsyncRequestManager is being used
            assert isinstance(
                driver.request_manager,
                ATBAsyncRequestManager,
            )

            # Verify initial rate was passed correctly
            assert driver.request_manager.config.initial_rate == 5.0
