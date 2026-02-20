"""Tests for Step 13: Archive Event Hook.

This module tests the on_archive callback hook that allows customization
of file archival behavior.

Key behaviors tested:
- default_archive_callback is used by default
- Custom on_archive callback can be provided
- Callback receives correct parameters (content, url, expected_type, storage_dir)
- Callback return value is used as file_url
- Integration with archive Request/ArchiveResponse flow
"""

from collections.abc import Generator
from pathlib import Path
from typing import Any

from kent.data_types import (
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
)
from kent.driver.sync_driver import (
    SyncDriver,
    default_archive_callback,
)
from tests.utils import collect_results


class TestDefaultArchiveCallback:
    """Tests for the default_archive_callback function."""

    def test_default_callback_extracts_filename_from_url(
        self, tmp_path: Path
    ) -> None:
        """The default_archive_callback shall extract filename from URL path."""
        content = b"test content"
        url = "http://example.com/files/test.pdf"

        file_url = default_archive_callback(
            content, url, expected_type="pdf", storage_dir=tmp_path
        )

        file_path = Path(file_url)
        assert file_path.name == "test.pdf"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    def test_default_callback_generates_filename(self, tmp_path: Path) -> None:
        """The default_archive_callback shall generate filename when URL has no path."""
        content = b"test content"
        url = "http://example.com/"

        file_url = default_archive_callback(
            content, url, expected_type="pdf", storage_dir=tmp_path
        )

        file_path = Path(file_url)
        assert file_path.name.startswith("download_")
        assert file_path.suffix == ".pdf"
        assert file_path.exists()
        assert file_path.read_bytes() == content

    def test_default_callback_uses_pdf_extension(self, tmp_path: Path) -> None:
        """The default_archive_callback shall use .pdf extension for pdf type."""
        content = b"test content"
        url = "http://example.com/"

        file_url = default_archive_callback(
            content, url, expected_type="pdf", storage_dir=tmp_path
        )

        file_path = Path(file_url)
        assert file_path.suffix == ".pdf"

    def test_default_callback_uses_mp3_extension(self, tmp_path: Path) -> None:
        """The default_archive_callback shall use .mp3 extension for audio type."""
        content = b"test audio"
        url = "http://example.com/"

        file_url = default_archive_callback(
            content, url, expected_type="audio", storage_dir=tmp_path
        )

        file_path = Path(file_url)
        assert file_path.suffix == ".mp3"


class TestCustomArchiveCallback:
    """Tests for custom on_archive callbacks."""

    def test_custom_callback_receives_content(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_archive callback shall receive file content."""

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

        # Track callback parameters
        callback_params: dict[Any, Any] = {}

        def custom_archive_callback(
            content: bytes,
            url: str,
            expected_type: str | None,
            storage_dir: Path,
        ) -> str:
            callback_params["content"] = content
            callback_params["url"] = url
            callback_params["expected_type"] = expected_type
            callback_params["storage_dir"] = storage_dir
            # Use default behavior
            return default_archive_callback(
                content, url, expected_type, storage_dir
            )

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_archive=custom_archive_callback,
        )

        driver.run()

        # Verify callback was called with correct parameters
        assert "content" in callback_params
        assert isinstance(callback_params["content"], bytes)
        assert callback_params["url"] == f"{server_url}/files/test.pdf"
        assert callback_params["expected_type"] == "pdf"
        assert callback_params["storage_dir"] == tmp_path

    def test_custom_callback_return_value_used_as_file_url(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_archive callback return value shall be used as file_url in ArchiveResponse."""

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

        def custom_archive_callback(
            content: bytes,
            url: str,
            expected_type: str | None,
            storage_dir: Path,
        ) -> str:
            # Return custom file path
            return "/custom/path/to/file.pdf"

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_archive=custom_archive_callback,
        )

        driver.run()

        # Verify custom path was used
        assert len(results) == 1
        assert results[0]["file_url"] == "/custom/path/to/file.pdf"

    def test_custom_callback_can_save_to_different_location(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_archive callback shall allow saving files to custom locations."""

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

        # Create custom storage directory
        custom_dir = tmp_path / "custom_storage"
        custom_dir.mkdir()

        def custom_archive_callback(
            content: bytes,
            url: str,
            expected_type: str | None,
            storage_dir: Path,
        ) -> str:
            # Save to custom directory instead of storage_dir
            file_path = custom_dir / "my_custom_file.pdf"
            file_path.write_bytes(content)
            return str(file_path)

        scraper = SimpleScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_archive=custom_archive_callback,
        )

        driver.run()

        # Verify file was saved to custom location
        custom_file = custom_dir / "my_custom_file.pdf"
        assert custom_file.exists()
        assert len(results) == 1
        assert results[0]["file_url"] == str(custom_file)


class TestArchiveCallbackIntegration:
    """Tests for on_archive callback integration with scraper flow."""

    def test_default_callback_used_when_none_provided(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The driver shall use default_archive_callback when on_archive is None."""

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

        # Don't provide on_archive callback
        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Verify file was saved with default behavior
        assert len(results) == 1
        file_path = Path(results[0]["file_url"])
        assert file_path.exists()
        assert file_path.parent == tmp_path

    def test_callback_called_for_each_archive_request(
        self, server_url: str, tmp_path: Path
    ) -> None:
        """The on_archive callback shall be called for each archive Request."""

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

        # Track callback invocations
        callback_count = {"count": 0}

        def counting_callback(
            content: bytes,
            url: str,
            expected_type: str | None,
            storage_dir: Path,
        ) -> str:
            callback_count["count"] += 1
            return default_archive_callback(
                content, url, expected_type, storage_dir
            )

        scraper = MultiFileScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_archive=counting_callback,
        )

        driver.run()

        # Verify callback was called twice (once for each archive Request)
        assert callback_count["count"] == 2
