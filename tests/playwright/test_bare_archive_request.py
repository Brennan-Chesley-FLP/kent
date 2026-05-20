"""Integration test for ``Request(archive=True)`` with no Via.

Exercises the bare-archive code path in PlaywrightDriver: a step yields a
direct HTTP archive Request (no ViaFormSubmit/ViaLink), and the driver
fetches it through the BrowserContext's APIRequestContext rather than
through ``page.goto()``. The mock server returns the PDF with
``Content-Disposition: attachment``, so the old ``page.goto`` path would
fail with ``Page.goto: Download is starting`` — this test pins the fix.
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from kent.common.decorators import step
from kent.data_types import (
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
)
from kent.driver.playwright_driver import PlaywrightDriver
from tests.conftest import AioHttpTestServer


class BareArchivePdfScraper(BaseScraper[None]):
    """Entry yields a navigating fetch; its continuation yields a bare archive."""

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.archive_responses: list[ArchiveResponse] = []
        self.local_paths: list[str] = []

    def get_entry(self):
        # Navigate to a real page first so the worker has a parent_request_id
        # to attach to. The archive request itself targets a different URL
        # (one served with Content-Disposition: attachment).
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/cases/BCC-2024-002",
            ),
            continuation=self.fetch_pdf,
        )

    @step
    def fetch_pdf(self):
        # Bare HTTP Request with archive=True and no Via — must round-trip
        # through APIRequestContext, not page.goto().
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/opinions/BCC-2024-002.pdf",
            ),
            continuation=self.collect,
            archive=True,
            expected_type="pdf",
        )

    @step
    def collect(self, response, local_filepath: str | None):
        # Capture the ArchiveResponse and the on-disk path so the test
        # body can assert against them.
        assert isinstance(response, ArchiveResponse), type(response).__name__
        self.archive_responses.append(response)
        if local_filepath is not None:
            self.local_paths.append(local_filepath)
        yield ParsedData(data={"path": local_filepath})


class TestBareArchiveRequest:
    """Pin the Request(archive=True, via=None) round-trip through Playwright."""

    @pytest.mark.asyncio
    async def test_bare_archive_fetches_attachment(
        self, bug_court_server: AioHttpTestServer
    ):
        scraper = BareArchivePdfScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
                storage_dir=Path(tmpdir) / "archive",
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()
                assert stats.errors.total == 0, (
                    f"Expected no errors but got {stats.errors.total}"
                )

            assert len(scraper.archive_responses) == 1
            archived = scraper.archive_responses[0]

            # Sanity: status, URL, and body actually round-tripped.
            assert archived.status_code == 200
            assert archived.url.endswith("/opinions/BCC-2024-002.pdf")
            assert archived.content.startswith(b"%PDF"), (
                f"Content doesn't look like a PDF (first 8 bytes: {archived.content[:8]!r})"
            )

            # The file_url points to a real file on disk and matches the
            # buffered bytes byte-for-byte. Asserted inside the tempdir so
            # the on-disk path still exists.
            assert archived.file_url
            on_disk = Path(archived.file_url)
            assert on_disk.is_file()
            assert on_disk.read_bytes() == archived.content
