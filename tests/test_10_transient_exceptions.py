"""Tests for Step 10: Transient Exceptions.

This module tests the transient exception handling introduced in Step 10:
1. TransientException base class for temporary errors that might resolve on retry
2. HTMLResponseAssumptionException for unexpected HTTP status codes (especially 5xx)
3. RequestTimeoutException for request timeouts
4. SyncDriver raising HTMLResponseAssumptionException for 5xx status codes
5. Mock server ?server_error=true endpoint returning 500 Internal Server Error

Tests use a real aiohttp server to verify actual HTTP behavior.
"""

import pytest

from kent.common.exceptions import (
    HTMLResponseAssumptionException,
    RequestTimeoutException,
    TransientException,
)


class TestTransientException:
    """Tests for TransientException base class."""

    def test_transient_exception_is_exception_subclass(self):
        """TransientException shall be a subclass of Exception."""
        assert issubclass(TransientException, Exception)

    def test_transient_exception_can_be_raised(self):
        """TransientException shall be raisable and catchable."""
        with pytest.raises(TransientException):
            raise TransientException("Test transient error")

    def test_transient_exception_message(self):
        """TransientException shall preserve the error message."""
        exc = TransientException("Network timeout")
        assert str(exc) == "Network timeout"


class TestHTMLResponseAssumptionException:
    """Tests for HTMLResponseAssumptionException."""

    def test_exception_has_required_attributes(self):
        """HTMLResponseAssumptionException shall have status_code, expected_codes, url, and message attributes."""
        exc = HTMLResponseAssumptionException(
            status_code=503,
            expected_codes=[200, 201],
            url="http://example.com/test",
        )

        assert exc.status_code == 503
        assert exc.expected_codes == [200, 201]
        assert exc.url == "http://example.com/test"
        assert exc.message is not None

    def test_exception_is_transient(self):
        """HTMLResponseAssumptionException shall be a subclass of TransientException."""
        assert issubclass(HTMLResponseAssumptionException, TransientException)

    def test_exception_message_includes_status_code(self):
        """HTMLResponseAssumptionException shall include actual status code in message."""
        exc = HTMLResponseAssumptionException(
            status_code=500,
            expected_codes=[200],
            url="http://example.com/api",
        )

        message = str(exc)
        assert "500" in message
        assert "http://example.com/api" in message

    def test_exception_message_includes_expected_codes(self):
        """HTMLResponseAssumptionException shall include expected codes in message."""
        exc = HTMLResponseAssumptionException(
            status_code=404,
            expected_codes=[200, 201, 204],
            url="http://example.com/resource",
        )

        message = str(exc)
        assert "200" in message
        assert "201" in message
        assert "204" in message

    def test_exception_message_with_single_expected_code(self):
        """HTMLResponseAssumptionException shall format message correctly with single expected code."""
        exc = HTMLResponseAssumptionException(
            status_code=503,
            expected_codes=[200],
            url="http://example.com/page",
        )

        message = str(exc)
        assert "HTTP 503" in message
        assert "expected one of: 200" in message

    def test_exception_message_with_multiple_expected_codes(self):
        """HTMLResponseAssumptionException shall format message correctly with multiple expected codes."""
        exc = HTMLResponseAssumptionException(
            status_code=500,
            expected_codes=[200, 201, 202],
            url="http://example.com/api",
        )

        message = str(exc)
        assert "HTTP 500" in message
        assert "expected one of:" in message


class TestRequestTimeoutException:
    """Tests for RequestTimeoutException."""

    def test_exception_has_required_attributes(self):
        """RequestTimeoutException shall have url, timeout_seconds, and message attributes."""
        exc = RequestTimeoutException(
            url="http://example.com/slow",
            timeout_seconds=30.5,
        )

        assert exc.url == "http://example.com/slow"
        assert exc.timeout_seconds == 30.5
        assert exc.message is not None

    def test_exception_is_transient(self):
        """RequestTimeoutException shall be a subclass of TransientException."""
        assert issubclass(RequestTimeoutException, TransientException)

    def test_exception_message_includes_url(self):
        """RequestTimeoutException shall include URL in message."""
        exc = RequestTimeoutException(
            url="http://example.com/endpoint",
            timeout_seconds=15.0,
        )

        message = str(exc)
        assert "http://example.com/endpoint" in message

    def test_exception_message_includes_timeout(self):
        """RequestTimeoutException shall include timeout duration in message."""
        exc = RequestTimeoutException(
            url="http://example.com/api",
            timeout_seconds=45.0,
        )

        message = str(exc)
        assert "45.0" in message or "45" in message
        assert "timed out" in message.lower()

    def test_exception_formats_timeout_with_decimal(self):
        """RequestTimeoutException shall format timeout duration with decimal precision."""
        exc = RequestTimeoutException(
            url="http://example.com/test",
            timeout_seconds=12.345,
        )

        message = str(exc)
        assert "12.345" in message


class TestMockServerErrorEndpoint:
    """Tests for mock server 500 error endpoint."""

    def test_server_error_endpoint_returns_500(self, server_url: str):
        """The mock server shall return 500 status code when ?server_error=true."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}?server_error=true")

        assert response.status_code == 500

    def test_server_error_endpoint_returns_error_html(self, server_url: str):
        """The mock server shall return error HTML when ?server_error=true."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}?server_error=true")

        assert "500 Internal Server Error" in response.text
        assert "server encountered an error" in response.text.lower()

    def test_server_error_endpoint_includes_reference(self, server_url: str):
        """The error response shall include the docket number as reference."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}?server_error=true")

        assert docket in response.text
        assert "Request ID:" in response.text

    def test_normal_endpoint_returns_200(self, server_url: str):
        """The mock server shall return 200 for normal requests without ?server_error."""
        import httpx

        docket = "BCC-2024-001"
        response = httpx.get(f"{server_url}/cases/{docket}")

        assert response.status_code == 200
        assert "Bug Civil Court" in response.text
        # Should NOT have error message
        assert "500 Internal Server Error" not in response.text


class TestSyncDriverRaisesHTTPResponseException:
    """Tests for SyncDriver raising HTMLResponseAssumptionException for 5xx errors."""

    def test_driver_raises_exception_on_500_error(
        self, server_url: str, tmp_path
    ):
        """The driver shall raise HTMLResponseAssumptionException when server returns 500."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Create a scraper that requests the error endpoint
        class ErrorEndpointScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?server_error=true",
                    ),
                    continuation="parse_response",
                )

            def parse_response(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                # This should never be called because driver raises before
                yield ParsedData({"status": response.status_code})

        scraper = ErrorEndpointScraper()
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should raise HTMLResponseAssumptionException
        with pytest.raises(HTMLResponseAssumptionException) as exc_info:
            driver.run()

        exc = exc_info.value
        assert exc.status_code == 500
        assert exc.url == f"{server_url}/cases/BCC-2024-001?server_error=true"
        assert 200 in exc.expected_codes

        # No data should have been collected
        assert len(results) == 0

    def test_driver_does_not_raise_exception_on_200(
        self, server_url: str, tmp_path
    ):
        """The driver shall not raise exception when server returns 200."""
        from kent.driver.sync_driver import SyncDriver
        from tests.scraper.example.bug_court import (
            BugCourtScraper,
        )
        from tests.utils import collect_results

        scraper = BugCourtScraper()
        scraper.BASE_URL = server_url
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should complete without exceptions
        driver.run()

        # Should have results
        assert len(results) > 0

    def test_driver_raises_exception_on_503_error(
        self, server_url: str, tmp_path
    ):
        """The driver shall raise HTMLResponseAssumptionException for any 5xx status code."""
        from collections.abc import Generator
        from unittest.mock import Mock

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Create a scraper
        class TestScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001",
                    ),
                    continuation="parse_response",
                )

            def parse_response(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({"status": response.status_code})

        scraper = TestScraper()
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Mock httpx to return 503 status code
        mock_response = Mock()
        mock_response.status_code = 503
        mock_response.headers = {}
        mock_response.content = b"Service Unavailable"
        mock_response.text = "Service Unavailable"

        # Patch the driver's client directly since it's created during __init__
        driver.request_manager._client.request = Mock(
            return_value=mock_response
        )  # ty: ignore[invalid-assignment]

        # Should raise HTMLResponseAssumptionException
        with pytest.raises(HTMLResponseAssumptionException) as exc_info:
            driver.run()

        exc = exc_info.value
        assert exc.status_code == 503

    def test_exception_includes_request_url(self, server_url: str, tmp_path):
        """The exception shall include the request URL that returned the error."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        class ErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?server_error=true",
                    ),
                    continuation="parse",
                )

            def parse(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({})

        scraper = ErrorScraper()
        callback, results = collect_results()
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        with pytest.raises(HTMLResponseAssumptionException) as exc_info:
            driver.run()

        exc = exc_info.value
        assert "BCC-2024-001" in exc.url
        assert "server_error=true" in exc.url


class TestTransientExceptionCallback:
    """Tests for on_transient_exception callback in SyncDriver."""

    def test_callback_receives_exception_and_can_stop(
        self, server_url: str, tmp_path
    ):
        """The driver shall invoke on_transient_exception callback and stop when it returns False."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Track callback invocations
        callback_invocations = []

        def transient_callback(exc: TransientException) -> bool:
            callback_invocations.append(exc)
            return False  # Stop scraping

        # Create a scraper that encounters transient error
        class TransientErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?server_error=true",
                    ),
                    continuation="parse",
                )

            def parse(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({"status": response.status_code})

        scraper = TransientErrorScraper()
        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_transient_exception=transient_callback,
        )

        # Run should not raise exception
        driver.run()

        # Callback should have been invoked once
        assert len(callback_invocations) == 1
        exc = callback_invocations[0]
        assert isinstance(exc, HTMLResponseAssumptionException)
        assert exc.status_code == 500

        # No results should have been collected
        assert len(results) == 0

    def test_callback_can_continue_scraping(self, server_url: str, tmp_path):
        """The driver shall continue scraping when on_transient_exception callback returns True."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Track callback invocations
        callback_invocations = []

        def transient_callback(exc: TransientException) -> bool:
            callback_invocations.append(exc)
            return True  # Continue scraping

        # Create a scraper that yields error request then normal request
        class MixedScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases",
                    ),
                    continuation="parse_list",
                )

            def parse_list(
                self, response: Response
            ) -> Generator[NavigatingRequest, None, None]:
                # First yield error request
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?server_error=true",
                    ),
                    continuation="parse_detail",
                )
                # Then yield normal request
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-002",
                    ),
                    continuation="parse_detail",
                )

            def parse_detail(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({"url": response.url})

        scraper = MixedScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_transient_exception=transient_callback,
        )

        # Run should not raise exception
        driver.run()

        # Callback should have been invoked once (for error request)
        assert len(callback_invocations) == 1
        assert isinstance(
            callback_invocations[0], HTMLResponseAssumptionException
        )

        # Should have results from the normal request (second request)
        assert len(results) == 1
        assert "BCC-2024-002" in results[0]["url"]

    def test_no_callback_raises_exception(self, server_url: str, tmp_path):
        """The driver shall raise exception when on_transient_exception is not provided."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Create a scraper that encounters transient error
        class ErrorScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001?server_error=true",
                    ),
                    continuation="parse",
                )

            def parse(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({})

        scraper = ErrorScraper()
        callback, results = collect_results()
        # No on_transient_exception callback
        driver = SyncDriver(scraper, storage_dir=tmp_path, on_data=callback)

        # Should raise exception
        with pytest.raises(HTMLResponseAssumptionException):
            driver.run()

    def test_timeout_exception_triggers_callback(
        self, server_url: str, tmp_path
    ):
        """The driver shall invoke callback for RequestTimeoutException."""
        from collections.abc import Generator

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            NavigatingRequest,
            ParsedData,
            Response,
        )
        from kent.driver.sync_driver import SyncDriver
        from tests.utils import collect_results

        # Track callback invocations
        callback_invocations = []

        def transient_callback(exc: TransientException) -> bool:
            callback_invocations.append(exc)
            return False  # Stop scraping

        # Create a scraper
        class TestScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[NavigatingRequest, None, None]:
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases/BCC-2024-001",
                    ),
                    continuation="parse",
                )

            def parse(
                self, response: Response
            ) -> Generator[ParsedData, None, None]:
                yield ParsedData({"status": response.status_code})

        scraper = TestScraper()
        callback, results = collect_results()
        driver = SyncDriver(
            scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_transient_exception=transient_callback,
        )

        # Mock httpx to raise TimeoutException
        from unittest.mock import Mock

        import httpx

        # Patch the driver's client directly since it's created during __init__
        mock_request = Mock(
            side_effect=httpx.TimeoutException("Request timed out")
        )
        driver.request_manager._client.request = (
            mock_request  # ty: ignore[invalid-assignment]
        )

        # Should not raise exception (callback handles it)
        driver.run()

        # Callback should have been invoked
        assert len(callback_invocations) == 1
        exc = callback_invocations[0]
        assert isinstance(exc, RequestTimeoutException)
        assert "BCC-2024-001" in exc.url
