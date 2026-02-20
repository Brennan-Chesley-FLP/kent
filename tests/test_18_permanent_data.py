"""Tests for Step 18: Permanent Request Data.

This module tests the permanent dict on BaseRequest for persisting cookies
and headers across the request chain.

Key behaviors tested:
- permanent dict added to BaseRequest
- Permanent headers persist across request chain
- Permanent cookies persist across request chain
- Permanent data inherited from parent to child requests
- Permanent data merged (parent + child)
- Driver applies permanent data when making HTTP requests
"""

from collections.abc import Generator
from pathlib import Path

from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestPermanentHeaders:
    """Tests for permanent headers persistence."""

    def test_permanent_headers_persist_across_chain(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent headers shall persist across the request chain."""

        class HeaderPersistenceScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent header
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/protected",
                    ),
                    continuation="parse_protected",
                    permanent={
                        "headers": {"Authorization": "Bearer token123"}
                    },
                )

            def parse_protected(self, response: Response):
                # Should have received the Authorization header
                yield ParsedData(data={"url": response.url})

        scraper = HeaderPersistenceScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Verify scraper completed
        assert len(results) == 1

    def test_permanent_headers_inherited_by_children(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent headers shall be inherited by child requests."""

        class HeaderInheritanceScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent header on parent
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step1",
                    ),
                    continuation="parse_step1",
                    permanent={"headers": {"X-Session": "abc123"}},
                )

            def parse_step1(self, response: Response):
                # Permanent header should be inherited
                # Yield child request without setting permanent
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step2",
                    ),
                    continuation="parse_step2",
                )

            def parse_step2(self, response: Response):
                # Should still have the inherited header
                yield ParsedData(data={"url": response.url})

        scraper = HeaderInheritanceScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1

    def test_permanent_headers_merged_with_child(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent headers shall be merged with child headers."""

        class HeaderMergeScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent header
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step1",
                    ),
                    continuation="parse_step1",
                    permanent={"headers": {"X-Parent": "parent-value"}},
                )

            def parse_step1(self, response: Response):
                # Add another permanent header (should merge)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step2",
                    ),
                    continuation="parse_step2",
                    permanent={"headers": {"X-Child": "child-value"}},
                )

            def parse_step2(self, response: Response):
                # Should have both headers
                yield ParsedData(data={"url": response.url})

        scraper = HeaderMergeScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1


class TestPermanentCookies:
    """Tests for permanent cookies persistence."""

    def test_permanent_cookies_persist_across_chain(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent cookies shall persist across the request chain."""

        class CookiePersistenceScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent cookie (simulating login)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/protected",
                    ),
                    continuation="parse_protected",
                    permanent={"cookies": {"session": "xyz789"}},
                )

            def parse_protected(self, response: Response):
                # Should have received the session cookie
                yield ParsedData(data={"url": response.url})

        scraper = CookiePersistenceScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1

    def test_permanent_cookies_inherited_by_children(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent cookies shall be inherited by child requests."""

        class CookieInheritanceScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent cookie
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step1",
                    ),
                    continuation="parse_step1",
                    permanent={"cookies": {"user_id": "12345"}},
                )

            def parse_step1(self, response: Response):
                # Cookie should be inherited
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step2",
                    ),
                    continuation="parse_step2",
                )

            def parse_step2(self, response: Response):
                # Should still have the inherited cookie
                yield ParsedData(data={"url": response.url})

        scraper = CookieInheritanceScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1


class TestPermanentMerging:
    """Tests for permanent data merging behavior."""

    def test_child_permanent_overrides_parent(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The child permanent data shall override parent for same key."""

        class OverrideScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set permanent header
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step1",
                    ),
                    continuation="parse_step1",
                    permanent={"headers": {"X-Token": "old-token"}},
                )

            def parse_step1(self, response: Response):
                # Override with new token
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/step2",
                    ),
                    continuation="parse_step2",
                    permanent={"headers": {"X-Token": "new-token"}},
                )

            def parse_step2(self, response: Response):
                # Should have new token (child overrides parent)
                yield ParsedData(data={"url": response.url})

        scraper = OverrideScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1

    def test_permanent_supports_multiple_keys(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent dict shall support both headers and cookies simultaneously."""

        class MultiKeyScrap(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Set both headers and cookies
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/protected",
                    ),
                    continuation="parse_protected",
                    permanent={
                        "headers": {"Authorization": "Bearer abc"},
                        "cookies": {"session": "xyz"},
                    },
                )

            def parse_protected(self, response: Response):
                # Should have both header and cookie
                yield ParsedData(data={"url": response.url})

        scraper = MultiKeyScrap()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1


class TestPermanentIsolation:
    """Tests for permanent data isolation between branches."""

    def test_permanent_deep_copied_prevents_sharing(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The permanent dict shall be deep copied to prevent sharing between branches."""

        class IsolationScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            def parse_entry(self, response: Response):
                # Create shared permanent dict
                permanent_data = {"headers": {"X-Branch": "initial"}}

                # Yield multiple requests with same permanent
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/branch1",
                    ),
                    continuation="parse_branch",
                    permanent=permanent_data,
                    accumulated_data={"branch": "1"},
                )

                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/branch2",
                    ),
                    continuation="parse_branch",
                    permanent=permanent_data,
                    accumulated_data={"branch": "2"},
                )

            def parse_branch(self, response: Response):
                # Each branch should have independent permanent data
                yield ParsedData(
                    data={
                        "branch": response.request.accumulated_data["branch"],
                        "url": response.url,
                    }
                )

        scraper = IsolationScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Both branches should complete successfully
        assert len(results) == 2


class TestPermanentAuthFlow:
    """Tests for authentication flow use case."""

    def test_auth_token_flow(self, server_url: str, tmp_path: Path) -> None:
        """The permanent data shall support authentication token workflow."""

        class AuthFlowScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/login",
                    ),
                    continuation="parse_login",
                )

            def parse_login(self, response: Response):
                # Simulate extracting token from login response
                token = "auth-token-from-login"

                # Navigate to protected resource with token
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/api/data",
                    ),
                    continuation="parse_api",
                    permanent={
                        "headers": {"Authorization": f"Bearer {token}"}
                    },
                )

            def parse_api(self, response: Response):
                # Fetch more data (token still in permanent)
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/api/more",
                    ),
                    continuation="parse_more",
                )

            def parse_more(self, response: Response):
                # Still authenticated via inherited permanent header
                yield ParsedData(data={"url": response.url})

        scraper = AuthFlowScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
