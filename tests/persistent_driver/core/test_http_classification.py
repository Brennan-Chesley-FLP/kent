"""Tests for the scraper-configurable HTTP status classification.

Covers:

- Active-set algebra on ``BaseScraper`` (defaults, overrides, precedence).
- Request-manager routing: transient → ``HTMLResponseAssumptionException``,
  persistent → ``PersistentHTTPResponseException``, otherwise a ``Response``.
- Dynamic classifier overrides that branch on headers / content.
- Worker behavior on a persistent HTTP response — request marked failed,
  error row written with ``error_type="persistent"``, no retries.
- ``PersistentException`` grouping: existing assumption exceptions still
  classify correctly (retrofit is backwards-compatible).
"""

from __future__ import annotations

from collections.abc import Generator, Mapping
from pathlib import Path
from unittest.mock import Mock

import pytest

from kent.common.exceptions import (
    HTMLResponseAssumptionException,
    HTMLStructuralAssumptionException,
    PersistentException,
    PersistentHTTPResponseException,
    TransientException,
)
from kent.common.request_manager import SyncRequestManager
from kent.data_types import (
    BaseRequest,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.driver.persistent_driver.errors import classify_error
from kent.driver.persistent_driver.persistent_driver import PersistentDriver
from kent.driver.persistent_driver.testing import (
    MockRequestManager,
)

# ---------------------------------------------------------------------------
# 1. Active-set algebra (pure unit tests — no HTTP)
# ---------------------------------------------------------------------------


class TestActiveSetAlgebra:
    def test_defaults(self) -> None:
        # 500 persistent, 503 transient, 200 successful out of the box.
        assert BaseScraper.is_persistent_error(500)
        assert BaseScraper.is_transient_error(503)
        assert not BaseScraper.is_persistent_error(200)
        assert not BaseScraper.is_transient_error(200)

    def test_move_503_to_persistent(self) -> None:
        class Scraper(BaseScraper[dict]):
            PERSISTENT_HTTP_ERROR_CODES = frozenset({503})

        assert Scraper.is_persistent_error(503)
        assert not Scraper.is_transient_error(503)
        # Other defaults unchanged.
        assert Scraper.is_transient_error(502)

    def test_move_500_to_transient(self) -> None:
        class Scraper(BaseScraper[dict]):
            TRANSIENT_HTTP_ERROR_CODES = frozenset({500})

        assert Scraper.is_transient_error(500)
        assert not Scraper.is_persistent_error(500)

    def test_successful_override_beats_everything(self) -> None:
        class Scraper(BaseScraper[dict]):
            SUCCESSFUL_HTTP_CODES = frozenset({503})

        assert not Scraper.is_transient_error(503)
        assert not Scraper.is_persistent_error(503)
        assert 503 in Scraper.active_successful_http_codes()

    def test_4xx_default_persistent(self) -> None:
        assert BaseScraper.is_persistent_error(404)
        assert BaseScraper.is_persistent_error(403)
        assert BaseScraper.is_persistent_error(400)
        # Transient 4xx codes stay transient.
        assert BaseScraper.is_transient_error(408)
        assert BaseScraper.is_transient_error(429)


# ---------------------------------------------------------------------------
# 2. Request manager routing
# ---------------------------------------------------------------------------


def _mock_http_response(
    status_code: int, headers: Mapping[str, str] | None = None
) -> Mock:
    m = Mock()
    m.status_code = status_code
    m.headers = headers or {}
    m.content = b"body"
    m.text = "body"
    return m


def _fake_request(url: str = "https://example.com/") -> BaseRequest:
    return BaseRequest(
        request=HTTPRequestParams(method=HttpMethod.GET, url=url),
        continuation="noop",
    )


class TestRequestManagerRouting:
    def test_transient_503_raises_html_response_exception(self) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_http_response(503))
        with pytest.raises(HTMLResponseAssumptionException) as ei:
            rm.resolve_request(_fake_request())
        assert ei.value.status_code == 503
        # Still a TransientException so retry logic applies.
        assert isinstance(ei.value, TransientException)

    def test_persistent_500_raises_persistent_exception(self) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_http_response(500))
        with pytest.raises(PersistentHTTPResponseException) as ei:
            rm.resolve_request(_fake_request())
        assert ei.value.status_code == 500
        # NOT a TransientException — retry logic will skip it.
        assert not isinstance(ei.value, TransientException)
        # IS a PersistentException so the worker can group-catch.
        assert isinstance(ei.value, PersistentException)

    def test_persistent_404_raises(self) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_http_response(404))
        with pytest.raises(PersistentHTTPResponseException):
            rm.resolve_request(_fake_request())

    def test_success_returns_response(self) -> None:
        rm = SyncRequestManager(scraper=BaseScraper)
        rm._client.request = Mock(return_value=_mock_http_response(200))
        resp = rm.resolve_request(_fake_request())
        assert resp.status_code == 200

    def test_override_makes_500_transient(self) -> None:
        class Scraper(BaseScraper[dict]):
            TRANSIENT_HTTP_ERROR_CODES = frozenset({500})

        rm = SyncRequestManager(scraper=Scraper)
        rm._client.request = Mock(return_value=_mock_http_response(500))
        with pytest.raises(HTMLResponseAssumptionException):
            rm.resolve_request(_fake_request())

    def test_override_makes_404_successful(self) -> None:
        class Scraper(BaseScraper[dict]):
            SUCCESSFUL_HTTP_CODES = frozenset({404})

        rm = SyncRequestManager(scraper=Scraper)
        rm._client.request = Mock(return_value=_mock_http_response(404))
        resp = rm.resolve_request(_fake_request())
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# 3. Dynamic classifier overrides (headers / content)
# ---------------------------------------------------------------------------


class TestDynamicClassifier:
    def test_headers_drive_classification(self) -> None:
        class Scraper(BaseScraper[dict]):
            @classmethod
            def is_transient_error(
                cls,
                status_code: int,
                headers: Mapping[str, str] | None = None,
                content: bytes | None = None,
            ) -> bool:
                # Treat 503 as transient only if a Retry-After header is present.
                if status_code == 503:
                    return bool(headers and "retry-after" in (headers or {}))
                return status_code in cls.active_transient_http_error_codes()

            @classmethod
            def is_persistent_error(
                cls,
                status_code: int,
                headers: Mapping[str, str] | None = None,
                content: bytes | None = None,
            ) -> bool:
                # 503 without Retry-After is persistent.
                if status_code == 503:
                    return not (headers and "retry-after" in (headers or {}))
                return status_code in cls.active_persistent_http_error_codes()

        rm = SyncRequestManager(scraper=Scraper)

        # With Retry-After → transient.
        rm._client.request = Mock(
            return_value=_mock_http_response(503, {"retry-after": "5"})
        )
        with pytest.raises(HTMLResponseAssumptionException):
            rm.resolve_request(_fake_request())

        # Without Retry-After → persistent.
        rm._client.request = Mock(return_value=_mock_http_response(503))
        with pytest.raises(PersistentHTTPResponseException):
            rm.resolve_request(_fake_request())

    def test_default_impls_tolerate_no_kwargs(self) -> None:
        assert BaseScraper.is_transient_error(503)
        assert BaseScraper.is_persistent_error(500)
        # Headers / content explicitly None.
        assert BaseScraper.is_transient_error(503, None, None)
        assert BaseScraper.is_persistent_error(500, None, None)


# ---------------------------------------------------------------------------
# 4. Worker behavior: persistent-error branch
# ---------------------------------------------------------------------------


class _PersistentScraper(BaseScraper[dict]):
    """Hits one URL and relies on the request manager to raise persistent."""

    def get_entry(self) -> Generator[Request, None, None]:
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url="https://example.com/missing"
            ),
            continuation="parse",
        )

    def parse(self, response: Response) -> Generator[ScraperYield, None, None]:
        yield ParsedData({"url": response.url})


class TestWorkerBranch:
    async def test_500_written_as_persistent_error_no_retries(
        self, db_path: Path
    ) -> None:
        manager = MockRequestManager()
        # MockRequestManager returns a Response directly; to exercise the
        # worker's `except PersistentHTTPResponseException` branch we wrap
        # resolve_request to raise instead.
        original_resolve = manager.resolve_request

        async def raising_resolve(req: BaseRequest) -> Response:  # noqa: ARG001
            raise PersistentHTTPResponseException(
                500, "https://example.com/missing"
            )

        manager.resolve_request = raising_resolve  # type: ignore[assignment]

        async with PersistentDriver.open(
            _PersistentScraper(),
            db_path,
            enable_monitor=False,
            request_manager=manager,
        ) as driver:
            await driver.run(setup_signal_handlers=False)

            # Inspect DB state.
            import sqlalchemy as sa

            async with driver.db._session_factory() as session:
                req_rows = (
                    await session.execute(
                        sa.text("SELECT status, retry_count FROM requests")
                    )
                ).all()
                err_rows = (
                    await session.execute(
                        sa.text("SELECT error_type, status_code FROM errors")
                    )
                ).all()

        # Our one request ended failed, without retries.
        assert len(req_rows) == 1
        assert req_rows[0][0] == "failed"
        assert req_rows[0][1] == 0
        # The error was recorded as "persistent".
        assert len(err_rows) == 1
        assert err_rows[0][0] == "persistent"
        assert err_rows[0][1] == 500

        # Silence the unused-variable warning for the helper reference.
        _ = original_resolve


# ---------------------------------------------------------------------------
# 5. PersistentException grouping is backwards-compatible
# ---------------------------------------------------------------------------


class TestPersistentExceptionGrouping:
    def test_structural_exception_classifies_as_structural(self) -> None:
        exc = HTMLStructuralAssumptionException(
            selector="//foo",
            selector_type="xpath",
            description="test",
            expected_min=1,
            expected_max=1,
            actual_count=0,
            request_url="https://example.com/",
        )
        # Subclass-specific branch still wins over PersistentException.
        assert classify_error(exc) == "structural"
        # But it also is a PersistentException for group-catch.
        assert isinstance(exc, PersistentException)

    def test_persistent_http_response_classifies_as_persistent(self) -> None:
        exc = PersistentHTTPResponseException(500, "https://example.com/")
        assert classify_error(exc) == "persistent"
        assert isinstance(exc, PersistentException)
