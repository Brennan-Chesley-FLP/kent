"""Tests for Step 13: Archive Handler.

This module tests the ArchiveHandler-based archive handling that controls
file download decisions and storage behavior.

Key behaviors tested:
- LocalSyncArchiveHandler is used by default
- Custom archive handlers can be provided
- Handler should_download is consulted before downloading
- Handler save receives correct parameters and its return value is used as file_url
- NoDownloadsSyncArchiveHandler skips all downloads
- Integration with archive Request/ArchiveResponse flow
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest

from kent.data_types import (
    ArchiveDecision,
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.archive_handler import (
    LocalSyncArchiveHandler,
    NoDownloadsSyncArchiveHandler,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results, collect_results_async


class TestLocalArchiveHandler:
    """Tests for the LocalSyncArchiveHandler."""

    def test_extracts_filename_from_url(self, tmp_path: Path) -> None:
        """The LocalSyncArchiveHandler shall extract filename from URL path."""
        handler = LocalSyncArchiveHandler(tmp_path)
        content = b"test content"
        url = "http://example.com/files/test.pdf"

        file_url = handler.save(
            url=url,
            deduplication_key=None,
            expected_type="pdf",
            hash_header_value=None,
            content=content,
        )

        file_path = Path(file_url)
        assert file_path.name == "test.pdf"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    def test_generates_filename_when_no_path(self, tmp_path: Path) -> None:
        """The LocalSyncArchiveHandler shall generate filename when URL has no path."""
        handler = LocalSyncArchiveHandler(tmp_path)
        content = b"test content"
        url = "http://example.com/"

        file_url = handler.save(
            url=url,
            deduplication_key=None,
            expected_type="pdf",
            hash_header_value=None,
            content=content,
        )

        file_path = Path(file_url)
        assert file_path.name.startswith("download_")
        assert file_path.suffix == ".pdf"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    def test_uses_pdf_extension(self, tmp_path: Path) -> None:
        """The LocalSyncArchiveHandler shall use .pdf extension for pdf type."""
        handler = LocalSyncArchiveHandler(tmp_path)
        content = b"test content"
        url = "http://example.com/"

        file_url = handler.save(
            url=url,
            deduplication_key=None,
            expected_type="pdf",
            hash_header_value=None,
            content=content,
        )

        file_path = Path(file_url)
        assert file_path.suffix == ".pdf"

    def test_uses_mp3_extension(self, tmp_path: Path) -> None:
        """The LocalSyncArchiveHandler shall use .mp3 extension for audio type."""
        handler = LocalSyncArchiveHandler(tmp_path)
        content = b"test audio"
        url = "http://example.com/"

        file_url = handler.save(
            url=url,
            deduplication_key=None,
            expected_type="audio",
            hash_header_value=None,
            content=content,
        )

        file_path = Path(file_url)
        assert file_path.suffix == ".mp3"

    def test_should_download_true_without_dedup_key(
        self, tmp_path: Path
    ) -> None:
        """shall download when no deduplication_key is provided."""
        handler = LocalSyncArchiveHandler(tmp_path)
        decision = handler.should_download(
            url="http://example.com/file.pdf",
            deduplication_key=None,
            expected_type="pdf",
            hash_header_value=None,
        )
        assert decision.download is True

    def test_should_download_true_when_dedup_dir_missing(
        self, tmp_path: Path
    ) -> None:
        """shall download when deduplication_key dir does not exist."""
        handler = LocalSyncArchiveHandler(tmp_path)
        decision = handler.should_download(
            url="http://example.com/file.pdf",
            deduplication_key="case-123",
            expected_type="pdf",
            hash_header_value=None,
        )
        assert decision.download is True

    def test_should_download_true_when_dedup_dir_empty(
        self, tmp_path: Path
    ) -> None:
        """shall download when deduplication_key dir exists but is empty."""
        handler = LocalSyncArchiveHandler(tmp_path)
        (tmp_path / "case-123").mkdir()
        decision = handler.should_download(
            url="http://example.com/file.pdf",
            deduplication_key="case-123",
            expected_type="pdf",
            hash_header_value=None,
        )
        assert decision.download is True

    def test_should_download_false_when_dedup_dir_has_files(
        self, tmp_path: Path
    ) -> None:
        """shall skip download when deduplication_key dir has files."""
        handler = LocalSyncArchiveHandler(tmp_path)
        dedup_dir = tmp_path / "case-123"
        dedup_dir.mkdir()
        existing_file = dedup_dir / "abc123.pdf"
        existing_file.write_bytes(b"existing content")

        decision = handler.should_download(
            url="http://example.com/file.pdf",
            deduplication_key="case-123",
            expected_type="pdf",
            hash_header_value=None,
        )
        assert decision.download is False
        assert decision.file_url == str(existing_file)

    def test_save_with_dedup_key_creates_subdirectory(
        self, tmp_path: Path
    ) -> None:
        """shall save to {storage_dir}/{dedup_key}/{filename}."""
        handler = LocalSyncArchiveHandler(tmp_path)
        content = b"test content"

        file_url = handler.save(
            url="http://example.com/files/abc123.pdf",
            deduplication_key="case-456",
            expected_type="pdf",
            hash_header_value=None,
            content=content,
        )

        file_path = Path(file_url)
        assert file_path.parent.name == "case-456"
        assert file_path.name == "abc123.pdf"
        assert file_path.exists()
        assert file_path.read_bytes() == content


class TestNoDownloadsArchiveHandler:
    """Tests for the NoDownloadsSyncArchiveHandler."""

    def test_should_download_always_false(self) -> None:
        """The NoDownloadsSyncArchiveHandler shall always skip downloads."""
        handler = NoDownloadsSyncArchiveHandler()
        decision = handler.should_download(
            url="http://example.com/file.pdf",
            deduplication_key=None,
            expected_type="pdf",
            hash_header_value=None,
        )
        assert decision.download is False
        assert decision.file_url == "skipped"


class TestCustomArchiveHandler:
    """Tests for custom archive handlers."""

    def test_custom_handler_receives_parameters(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """A custom archive handler shall receive correct parameters."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_file",
                )

            def archive_file(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=response.url,
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        save_params: dict[str, Any] = {}

        class TrackingHandler:
            def should_download(
                self, url, deduplication_key, expected_type, hash_header_value
            ):
                return ArchiveDecision(download=True)

            def save(
                self,
                url,
                deduplication_key,
                expected_type,
                hash_header_value,
                content,
            ):
                save_params["url"] = url
                save_params["expected_type"] = expected_type
                save_params["content"] = content
                file_path = tmp_path / "tracked.pdf"
                file_path.write_bytes(content)
                return str(file_path)

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            archive_handler=TrackingHandler(),
        )

        driver.run()

        assert "content" in save_params
        assert isinstance(save_params["content"], bytes)
        assert save_params["url"] == f"{server_url}/files/test.pdf"
        assert save_params["expected_type"] == "pdf"

    def test_custom_handler_return_value_used_as_file_url(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The handler save return value shall be used as file_url in ArchiveResponse."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_file",
                )

            def archive_file(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=response.url,
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        class CustomPathHandler:
            def should_download(
                self, url, deduplication_key, expected_type, hash_header_value
            ):
                return ArchiveDecision(download=True)

            def save(
                self,
                url,
                deduplication_key,
                expected_type,
                hash_header_value,
                content,
            ):
                return "/custom/path/to/file.pdf"

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            archive_handler=CustomPathHandler(),
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["file_url"] == "/custom/path/to/file.pdf"

    def test_should_download_false_skips_fetch(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """When should_download returns False, the driver shall skip the HTTP fetch."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_file",
                )

            def archive_file(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=response.url,
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(
                    data={
                        "file_url": response.file_url,
                        "status_code": response.status_code,
                    }
                )

        class SkipHandler:
            def should_download(
                self, url, deduplication_key, expected_type, hash_header_value
            ):
                return ArchiveDecision(
                    download=False, file_url="/existing/file.pdf"
                )

            def save(
                self,
                url,
                deduplication_key,
                expected_type,
                hash_header_value,
                content,
            ):
                raise AssertionError("save should not be called when skipping")

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            archive_handler=SkipHandler(),
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["file_url"] == "/existing/file.pdf"
        assert results[0]["status_code"] == 200


class TestArchiveHandlerIntegration:
    """Tests for archive handler integration with scraper flow."""

    def test_default_handler_used_when_none_provided(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall use LocalSyncArchiveHandler when no handler is provided."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_file",
                )

            def archive_file(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=response.url,
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        # File is saved under {tmp_path}/{deduplication_key}/{filename}
        assert tmp_path in file_path.parents

    def test_handler_called_for_each_archive_request(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The archive handler shall be called for each archive Request."""

        class MultiFileScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_first",
                )

            def archive_first(self, response: Response):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="archive_second",
                    expected_type="pdf",
                    archive=True,
                )

            def archive_second(self, response: ArchiveResponse):
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        save_count = {"count": 0}
        local_handler = LocalSyncArchiveHandler(tmp_path)

        class CountingHandler:
            def should_download(
                self, url, deduplication_key, expected_type, hash_header_value
            ):
                return ArchiveDecision(download=True)

            def save(
                self,
                url,
                deduplication_key,
                expected_type,
                hash_header_value,
                content,
            ):
                save_count["count"] += 1
                return local_handler.save(
                    url,
                    deduplication_key,
                    expected_type,
                    hash_header_value,
                    content,
                )

        scraper = MultiFileScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            archive_handler=CountingHandler(),
        )

        driver.run()

        assert save_count["count"] == 2

    def test_dedup_key_creates_subdirectory_in_file_path(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """Archive with explicit deduplication_key shall save under that subdirectory."""

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                    deduplication_key="test_filename",
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        assert "test_filename" in file_path.parts

    @pytest.mark.asyncio
    async def test_dedup_key_creates_subdirectory_async_driver(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """AsyncDriver archive with deduplication_key shall save under that subdirectory."""
        from kent.driver.async_driver import AsyncDriver

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                    deduplication_key="test_filename",
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        scraper = SimpleScraper()
        callback, results = collect_results_async()

        driver = AsyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        await driver.run()

        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        assert "test_filename" in file_path.parts

    @pytest.mark.asyncio
    async def test_dedup_key_creates_subdirectory_persistent_driver(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """PersistentDriver archive with deduplication_key shall save under that subdirectory."""
        from kent.driver.persistent_driver.persistent_driver import (
            PersistentDriver,
        )
        from kent.driver.persistent_driver.testing import (
            MockRequestManager,
            MockResponse,
        )

        class SimpleScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/test.pdf",
                    ),
                    continuation="parse_archive",
                    expected_type="pdf",
                    archive=True,
                    deduplication_key="test_filename",
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        request_manager = MockRequestManager()
        request_manager.add_response(
            f"{server_url}/files/test.pdf",
            MockResponse(
                content=b"%PDF-1.4 test content",
                status_code=200,
                headers={"Content-Type": "application/pdf"},
            ),
        )

        scraper = SimpleScraper()
        db_path = tmp_path / "test.db"

        async with PersistentDriver.open(
            scraper,
            db_path,
            enable_monitor=False,
            request_manager=request_manager,
        ) as driver:
            callback, results = collect_results_async()
            driver.on_data = callback
            await driver.run()

        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        assert "test_filename" in file_path.parts

    @pytest.mark.asyncio
    async def test_dedup_key_creates_subdirectory_playwright_driver(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """PlaywrightDriver archive with deduplication_key shall save under that subdirectory."""
        pytest.importorskip("playwright")

        from kent.common.decorators import step
        from kent.common.lxml_page_element import LxmlPageElement
        from kent.driver.playwright_driver import PlaywrightDriver

        class DownloadScraper(BaseScraper[dict]):
            def __init__(self, base_url: str):
                super().__init__()
                self.base_url = base_url

            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{self.base_url}/cases/BCC-2024-002",
                    ),
                    continuation=self.download_opinion,
                )

            @step
            def download_opinion(self, page: LxmlPageElement):
                links = page.query_css(
                    'a[href$=".pdf"]', "opinion link", min_count=1
                )
                href = links[0].get_attribute("href")
                assert href is not None
                from kent.common.page_element import ViaLink

                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=href,
                    ),
                    continuation="parse_archive",
                    archive=True,
                    expected_type="pdf",
                    deduplication_key="test_filename",
                    via=ViaLink(
                        selector='a[href$=".pdf"]',
                        description="opinion PDF link",
                    ),
                )

            def parse_archive(self, response: ArchiveResponse):
                yield ParsedData(data={"file_url": response.file_url})

        scraper = DownloadScraper(server_url)
        db_path = tmp_path / "test.db"
        storage_dir = tmp_path / "files"

        async with PlaywrightDriver.open(
            scraper,
            db_path,
            headless=True,
            enable_monitor=False,
            storage_dir=storage_dir,
        ) as driver:
            callback, results = collect_results_async()
            driver.on_data = callback
            await driver.run(setup_signal_handlers=False)

        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        assert "test_filename" in file_path.parts
