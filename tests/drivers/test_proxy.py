"""Tests for optional SOCKS5 / HTTP proxy wiring across drivers.

These tests don't make real network calls — they assert that a proxy URL
passed to the driver or request manager propagates to the underlying
httpx client / Playwright launch kwargs as expected.
"""

from __future__ import annotations

import pytest

from kent.common.request_manager import AsyncRequestManager, SyncRequestManager
from kent.driver.playwright_driver.playwright_driver import (
    _parse_proxy_for_playwright,
)


class TestParsePlaywrightProxy:
    def test_bare_socks5(self) -> None:
        assert _parse_proxy_for_playwright("socks5://proxy.example:1080") == {
            "server": "socks5://proxy.example:1080",
        }

    def test_http_with_credentials(self) -> None:
        assert _parse_proxy_for_playwright(
            "http://alice:s3cret@proxy.example:3128"
        ) == {
            "server": "http://proxy.example:3128",
            "username": "alice",
            "password": "s3cret",
        }

    def test_credentials_are_percent_decoded(self) -> None:
        # Real-world creds often contain reserved chars
        result = _parse_proxy_for_playwright(
            "socks5://al%40ice:p%3Ass@proxy.example:1080"
        )
        assert result["username"] == "al@ice"
        assert result["password"] == "p:ss"

    def test_no_port(self) -> None:
        assert _parse_proxy_for_playwright("http://proxy.example") == {
            "server": "http://proxy.example",
        }

    def test_invalid_url_raises(self) -> None:
        with pytest.raises(ValueError):
            _parse_proxy_for_playwright("not-a-url")


def _client_uses_proxy(client: object) -> bool:
    """Return True if an httpx client has any proxy mount wired up."""
    mounts = getattr(client, "_mounts", None)
    return bool(mounts)


class TestSyncRequestManagerProxy:
    def test_no_proxy_by_default(self) -> None:
        rm = SyncRequestManager()
        assert not _client_uses_proxy(rm._client)

    def test_default_client_gets_proxy(self) -> None:
        rm = SyncRequestManager(proxy="socks5://127.0.0.1:1080")
        assert _client_uses_proxy(rm._client)

    def test_alt_verify_client_gets_proxy(self) -> None:
        rm = SyncRequestManager(proxy="socks5://127.0.0.1:1080")
        alt = rm._client_for(False)
        assert _client_uses_proxy(alt)

    def test_bypass_client_gets_proxy(self) -> None:
        rm = SyncRequestManager(proxy="socks5://127.0.0.1:1080")
        bypass = rm._bypass_client_for(True)
        assert _client_uses_proxy(bypass)


class TestAsyncRequestManagerProxy:
    def test_no_proxy_by_default(self) -> None:
        rm = AsyncRequestManager()
        assert not _client_uses_proxy(rm._client)

    def test_default_client_gets_proxy(self) -> None:
        rm = AsyncRequestManager(proxy="socks5://127.0.0.1:1080")
        assert _client_uses_proxy(rm._client)

    def test_alt_verify_client_gets_proxy(self) -> None:
        rm = AsyncRequestManager(proxy="socks5://127.0.0.1:1080")
        alt = rm._client_for(False)
        assert _client_uses_proxy(alt)

    def test_bypass_client_gets_proxy(self) -> None:
        rm = AsyncRequestManager(proxy="socks5://127.0.0.1:1080")
        bypass = rm._bypass_client_for(True)
        assert _client_uses_proxy(bypass)


class TestDriverProxyPassthrough:
    """The driver kwarg should flow into the auto-constructed request manager."""

    def test_sync_driver_passes_proxy(self) -> None:
        from kent.data_types import BaseScraper
        from kent.driver.sync_driver import SyncDriver

        class _Scraper(BaseScraper[None]):
            pass

        driver = SyncDriver(_Scraper(), proxy="socks5://127.0.0.1:1080")
        assert _client_uses_proxy(driver.request_manager._client)

    def test_async_driver_passes_proxy(self) -> None:
        from kent.data_types import BaseScraper
        from kent.driver.async_driver import AsyncDriver

        class _Scraper(BaseScraper[None]):
            pass

        driver = AsyncDriver(_Scraper(), proxy="socks5://127.0.0.1:1080")
        assert _client_uses_proxy(driver.request_manager._client)
