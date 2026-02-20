"""Tests for request type serialization and deserialization round-trips."""

from __future__ import annotations

import sqlalchemy as sa


class TestRequestTypeRoundTrip:
    """Tests for request type serialization and deserialization round-trips."""

    async def test_navigating_request_round_trip(self, initialized_db) -> None:
        """Test that a navigating Request is correctly serialized and deserialized."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        # Create a navigating Request with all fields populated
        original = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/page",
                headers={"User-Agent": "Test", "Accept": "text/html"},
                cookies={"session": "abc123"},
            ),
            continuation="parse_page",
            current_location="https://example.com",
            accumulated_data={"key": "value", "count": 42},
            aux_data={"token": "xyz789"},
            permanent={"headers": {"Authorization": "Bearer token"}},
            priority=5,
        )

        # Serialize using the driver's method
        # We need a minimal driver instance just for serialization
        class MockScraper:
            pass

        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        # Verify request_type is set correctly
        assert serialized["request_type"] == "navigating"
        assert serialized["expected_type"] is None

        # Insert into database
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        # Retrieve and deserialize
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        assert row is not None

        deserialized = driver._deserialize_request(row)

        # Verify it's the correct type
        assert isinstance(deserialized, Request)
        assert not deserialized.nonnavigating
        assert not deserialized.archive

        # Verify all fields match
        assert deserialized.request.method == original.request.method
        assert deserialized.request.url == original.request.url
        assert deserialized.request.headers == original.request.headers
        assert deserialized.request.cookies == original.request.cookies
        assert deserialized.continuation == original.continuation
        assert deserialized.current_location == original.current_location
        assert deserialized.accumulated_data == original.accumulated_data
        assert deserialized.aux_data == original.aux_data
        assert deserialized.permanent == original.permanent
        assert deserialized.priority == original.priority

    async def test_non_navigating_request_round_trip(
        self, initialized_db
    ) -> None:
        """Test that a non-navigating Request is correctly serialized and deserialized."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        # Create a non-navigating Request with all fields populated
        # Note: Use non-JSON bytes to test raw binary preservation.
        # JSON-like bytes get decoded to dicts by design (for form data).
        original = Request(
            nonnavigating=True,
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url="https://api.example.com/data",
                headers={"Content-Type": "application/octet-stream"},
                data=b"\x00\x01\x02\x03binary data\xff\xfe",
            ),
            continuation="process_api_response",
            current_location="https://example.com/main",
            accumulated_data={"items": [1, 2, 3]},
            aux_data={"page": 2},
            permanent={"cookies": {"auth": "secret"}},
            priority=3,
        )

        # Serialize
        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        # Verify request_type is set correctly
        assert serialized["request_type"] == "non_navigating"
        assert serialized["expected_type"] is None

        # Insert into database
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        # Retrieve and deserialize
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        assert row is not None

        deserialized = driver._deserialize_request(row)

        # Verify it's the correct type
        assert isinstance(deserialized, Request)
        assert deserialized.nonnavigating

        # Verify all fields match
        assert deserialized.request.method == original.request.method
        assert deserialized.request.url == original.request.url
        assert deserialized.request.headers == original.request.headers
        assert deserialized.request.data == original.request.data
        assert deserialized.continuation == original.continuation
        assert deserialized.current_location == original.current_location
        assert deserialized.accumulated_data == original.accumulated_data
        assert deserialized.aux_data == original.aux_data
        assert deserialized.permanent == original.permanent
        assert deserialized.priority == original.priority

    async def test_archive_request_round_trip(self, initialized_db) -> None:
        """Test that an archive Request is correctly serialized and deserialized."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        # Create an archive Request with all fields populated
        original = Request(
            archive=True,
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/files/document.pdf",
                headers={"Accept": "application/pdf"},
            ),
            continuation="handle_download",
            current_location="https://example.com/documents",
            expected_type="pdf",
            accumulated_data={"document_id": "12345"},
            aux_data={"filename": "document.pdf"},
            permanent={},
            priority=1,  # Default for archive Request
        )

        # Serialize
        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        # Verify request_type and expected_type are set correctly
        assert serialized["request_type"] == "archive"
        assert serialized["expected_type"] == "pdf"

        # Insert into database
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        # Retrieve and deserialize
        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        assert row is not None

        deserialized = driver._deserialize_request(row)

        # Verify it's the correct type
        assert isinstance(deserialized, Request)
        assert deserialized.archive

        # Verify all fields match
        assert deserialized.request.method == original.request.method
        assert deserialized.request.url == original.request.url
        assert deserialized.request.headers == original.request.headers
        assert deserialized.continuation == original.continuation
        assert deserialized.current_location == original.current_location
        assert deserialized.expected_type == original.expected_type
        assert deserialized.accumulated_data == original.accumulated_data
        assert deserialized.aux_data == original.aux_data
        assert deserialized.permanent == original.permanent
        assert deserialized.priority == original.priority

    async def test_archive_request_without_expected_type(
        self, initialized_db
    ) -> None:
        """Test archive Request round-trip when expected_type is None."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        # Create an archive Request without expected_type
        original = Request(
            archive=True,
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com/files/unknown",
            ),
            continuation="handle_download",
            current_location="https://example.com",
            expected_type=None,  # No type hint
        )

        # Serialize
        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        assert serialized["request_type"] == "archive"
        assert serialized["expected_type"] is None

        # Insert and retrieve
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        deserialized = driver._deserialize_request(row)

        assert isinstance(deserialized, Request)
        assert deserialized.archive
        assert deserialized.expected_type is None

    async def test_request_with_binary_body(self, initialized_db) -> None:
        """Test request round-trip with binary body data."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        binary_body = b"\x00\x01\x02\xff\xfe\xfd"

        original = Request(
            nonnavigating=True,
            request=HTTPRequestParams(
                method=HttpMethod.POST,
                url="https://example.com/upload",
                data=binary_body,
            ),
            continuation="handle_upload",
            current_location="",
        )

        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        # Insert and retrieve
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        result = driver._deserialize_request(row)
        # Request returns BaseRequest directly
        deserialized = result if not isinstance(result, tuple) else result[0]

        assert deserialized.request.data == binary_body

    async def test_request_with_empty_optional_fields(
        self, initialized_db
    ) -> None:
        """Test request round-trip with minimal fields (empty optionals)."""
        from kent.data_types import (
            HttpMethod,
            HTTPRequestParams,
            Request,
        )
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )

        # Minimal request with empty optional fields
        original = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="https://example.com",
            ),
            continuation="parse",
            current_location="",
        )

        driver = PersistentDriver.__new__(PersistentDriver)
        serialized = driver._serialize_request(original)

        # Verify optional fields are None/empty
        assert serialized["headers_json"] is None
        assert serialized["cookies_json"] is None
        assert serialized["body"] is None
        assert serialized["accumulated_data_json"] is None
        assert serialized["aux_data_json"] is None
        assert serialized["permanent_json"] is None

        # Insert and retrieve
        engine, session_factory = initialized_db
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO requests (
                    status, priority, queue_counter, request_type,
                    method, url, headers_json, cookies_json, body,
                    continuation, current_location,
                    accumulated_data_json, aux_data_json, permanent_json,
                    expected_type
                ) VALUES (
                    'pending', :priority, 1, :request_type,
                    :method, :url, :headers_json, :cookies_json, :body,
                    :continuation, :current_location,
                    :accumulated_data_json, :aux_data_json, :permanent_json,
                    :expected_type
                )
                """),
                {
                    "priority": original.priority,
                    "request_type": serialized["request_type"],
                    "method": serialized["method"],
                    "url": serialized["url"],
                    "headers_json": serialized["headers_json"],
                    "cookies_json": serialized["cookies_json"],
                    "body": serialized["body"],
                    "continuation": serialized["continuation"],
                    "current_location": serialized["current_location"],
                    "accumulated_data_json": serialized[
                        "accumulated_data_json"
                    ],
                    "aux_data_json": serialized["aux_data_json"],
                    "permanent_json": serialized["permanent_json"],
                    "expected_type": serialized["expected_type"],
                },
            )
            await session.commit()

        async with session_factory() as session:
            result = await session.execute(
                sa.text("""
                SELECT id, request_type, method, url, headers_json, cookies_json, body,
                       continuation, current_location,
                       accumulated_data_json, aux_data_json, permanent_json,
                       expected_type, priority,
                       is_speculative, speculation_id
                FROM requests WHERE id = 1
                """)
            )
            row = result.first()
        result = driver._deserialize_request(row)
        # Request returns BaseRequest directly
        deserialized = result if not isinstance(result, tuple) else result[0]

        # Verify deserialized correctly with empty defaults
        assert deserialized.request.headers is None
        assert deserialized.request.cookies is None
        assert deserialized.request.data is None
        assert deserialized.accumulated_data == {}
        assert deserialized.aux_data == {}
        assert deserialized.permanent == {}
