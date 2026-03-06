"""Integration test: PlaywrightDriver with form.submit() via the Bug Court server."""

import tempfile
from dataclasses import replace
from pathlib import Path

import pytest

from kent.common.decorators import step
from kent.common.lxml_page_element import LxmlPageElement
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
)
from kent.driver.playwright_driver import PlaywrightDriver
from tests.conftest import AioHttpTestServer


class FormSubmitScraper(BaseScraper[None]):
    """Scraper that navigates to a search form, submits it, and parses results."""

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.parsed_dockets: list[str] = []

    def get_entry(self):
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/search",
            ),
            continuation=self.submit_search,
        )

    @step
    def submit_search(self, page: LxmlPageElement):
        form = page.find_form("#case-search", "case search form")
        request = form.submit(data={"case_type": "Property Dispute", "status": ""})
        yield replace(request, continuation=self.parse_results)

    @step
    def parse_results(self, page: LxmlPageElement):
        rows = page.query_css(".case-row", "result rows", min_count=1)
        for row in rows:
            docket_el = row.query_css(".docket", "docket", min_count=1)
            name_el = row.query_css(".case-name", "case name", min_count=1)
            docket = docket_el[0].text_content()
            self.parsed_dockets.append(docket)
            yield ParsedData(
                data={
                    "docket": docket,
                    "case_name": name_el[0].text_content(),
                },
            )


class ComplexFormScraper(BaseScraper[None]):
    """Scraper exercising hidden, radio, select, and invisible form fields."""

    def __init__(self, base_url: str):
        super().__init__()
        self.base_url = base_url
        self.parsed_dockets: list[str] = []
        self.viewstate_ok: str = ""
        self.client_state_ok: str = ""
        self.category: str = ""

    def get_entry(self):
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.base_url}/complex-search",
            ),
            continuation=self.submit_form,
        )

    @step
    def submit_form(self, page: LxmlPageElement):
        form = page.find_form("#Form1", "complex search form")
        request = form.submit(
            data={
                # Override the select
                "case_type": "Contract Dispute",
                # Set the visible date display
                "date_start_display": "01/01/2024",
                # Set the invisible parent input (Telerik-style)
                "date_start_hidden": "2024-01-01",
                # Set the hidden ClientState (what server reads)
                "date_start_client_state": '{"date":"2024-01-01"}',
            },
        )
        yield replace(request, continuation=self.parse_results)

    @step
    def parse_results(self, page: LxmlPageElement):
        # Verify hidden fields were submitted
        vs_els = page.query_css("#viewstate-ok", "viewstate check", min_count=1)
        self.viewstate_ok = vs_els[0].text_content()

        cs_els = page.query_css(
            "#client-state-ok", "client state check", min_count=1
        )
        self.client_state_ok = cs_els[0].text_content()

        cat_els = page.query_css("#category", "category", min_count=1)
        self.category = cat_els[0].text_content()

        rows = page.query_css(".case-row", "result rows", min_count=0)
        for row in rows:
            docket_el = row.query_css(".docket", "docket", min_count=1)
            self.parsed_dockets.append(docket_el[0].text_content())
            yield ParsedData(data={"docket": docket_el[0].text_content()})


class TestPlaywrightFormSubmit:
    """Test PlaywrightDriver form submission via the Bug Court mock server."""

    @pytest.mark.asyncio
    async def test_form_submit_filters_cases(
        self, bug_court_server: AioHttpTestServer
    ):
        """Submit the search form with a case_type filter and verify results."""
        scraper = FormSubmitScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()
                # "Property Dispute" matches exactly BCC-2024-001
                assert stats.results.total == 1
                assert scraper.parsed_dockets == ["BCC-2024-001"]

    @pytest.mark.asyncio
    async def test_complex_form_with_hidden_and_radio_fields(
        self, bug_court_server: AioHttpTestServer
    ):
        """Submit a form with hidden inputs, radios, selects, and invisible fields.

        Verifies that _execute_via_navigation correctly handles:
        - Hidden inputs (ViewState, ClientState) via JS evaluate
        - Radio buttons via checked property
        - Select dropdowns via select_option
        - Invisible inputs (Telerik-style parent) via JS evaluate
        """
        scraper = ComplexFormScraper(bug_court_server.url)

        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test.db"

            async with PlaywrightDriver.open(
                scraper,
                db_path,
                headless=True,
                enable_monitor=False,
            ) as driver:
                await driver.run(setup_signal_handlers=False)

                stats = await driver.get_stats()

                # Hidden __VIEWSTATE was submitted
                assert scraper.viewstate_ok == "yes"
                # Hidden date_start_client_state was submitted
                assert scraper.client_state_ok == "yes"
                # Radio default "civil" was preserved
                assert scraper.category == "civil"
                # "Contract Dispute" filter matched cases
                assert stats.results.total >= 1
                assert "BCC-2024-003" in scraper.parsed_dockets
