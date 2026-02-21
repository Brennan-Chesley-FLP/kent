"""Bug Civil Court demo scraper.

This scraper showcases the kent framework's features by extracting data
from the Bug Civil Court demo website.  It exercises:

- ``YearlySpeculation`` for speculative case discovery
- ``page`` (LxmlPageElement / PageElement protocol) for HTML parsing
- ``json_content`` for JSON API parsing
- ``Request(nonnavigating=True)`` for side-channel API fetches
- ``Request(archive=True)`` for downloading audio and images
- ``accumulated_data`` for passing context through request chains
- ``EstimateData`` for integrity checking
- ``ScrapedData`` with deferred validation
- ``fails_successfully()`` for soft-404 detection
- ``page.find_form()`` / ``Form.submit()`` for form-based search
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import replace
from datetime import timedelta

from pyrate_limiter import Duration, Rate

from kent.common.decorators import entry, step
from kent.common.lxml_page_element import LxmlPageElement
from kent.common.param_models import DateRange
from kent.common.speculation_types import (
    YearlySpeculation,
    YearPartition,
)
from kent.data_types import (
    BaseScraper,
    EstimateData,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.demo.models import (
    CaseData,
    JusticeData,
    OpinionData,
    OralArgumentData,
)

# Type alias for the union of all data types this scraper produces.
DemoData = CaseData | JusticeData | OralArgumentData | OpinionData


class BugCourtDemoScraper(BaseScraper[DemoData]):
    """Scraper for the Bug Civil Court demo website.

    Demonstrates speculative requests, HTML page parsing via the
    PageElement protocol, JSON API consumption, file archiving,
    and Pydantic data validation.
    """

    court_url = "http://127.0.0.1:8080"
    rate_limits = [Rate(1, Duration.SECOND)]

    # ── Entry points ────────────────────────────────────────────

    @entry(
        CaseData,
        speculative=YearlySpeculation(
            backfill=(
                YearPartition(year=2024, number=(1, 10), frozen=True),
                YearPartition(year=2025, number=(1, 10), frozen=True),
                YearPartition(year=2026, number=(1, 10), frozen=False),
            ),
            trailing_period=timedelta(days=60),
            largest_observed_gap=3,
        ),
    )
    def fetch_case(self, year: int, number: int) -> Request:
        """Speculative case fetcher — probes ``/cases/{year}/{number}``.

        The driver generates ``(year, number)`` pairs from the
        YearlySpeculation config and calls this for each pair.
        """
        return Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/cases/{year}/{number}",
            ),
            continuation=self.parse_case_detail,
        )

    @entry(OralArgumentData)
    def get_oral_arguments(
        self,
    ) -> Generator[Request, None, None]:
        """Start from the oral-arguments list page."""
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/oral-arguments",
            ),
            continuation=self.parse_oral_arguments_list,
        )

    @entry(JusticeData)
    def get_justices(self) -> Generator[Request, None, None]:
        """Fetch justice bios from the JSON API (nonnavigating)."""
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/api/justices",
            ),
            continuation=self.parse_justices_json,
            nonnavigating=True,
        )

    @entry(CaseData)
    def cases_by_date_filed(
        self,
        date_range: DateRange,
    ) -> Generator[Request, None, None]:
        """Search for cases by date-filed range via the search form.

        Navigates to the search page, then submits the date-search form
        using ``find_form`` / ``Form.submit()``.
        """
        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.court_url}/cases/search",
            ),
            continuation=self.submit_date_search,
            accumulated_data={
                "from_date": date_range.start.isoformat(),
                "to_date": date_range.end.isoformat(),
            },
        )

    # ── Speculation helper ──────────────────────────────────────

    def fails_successfully(self, response: Response) -> bool:
        """Detect soft-404 pages returned with a 200 status.

        The demo website returns a page containing "Case Not Found"
        for invalid docket numbers.
        """
        return "Case Not Found" not in response.text

    # ── Case parsing ────────────────────────────────────────────

    @step
    def parse_case_detail(
        self,
        page: LxmlPageElement,
        response: Response,
    ) -> Generator[ScraperYield, None, None]:
        """Parse a case detail page.

        Uses ``page.query_xpath()`` with count validation and
        ``page.find_links()`` to discover opinion/oral-argument
        links.  Yields ``CaseData`` and follow-on archive requests.

        Gracefully handles soft-404 pages (speculative misses) by
        checking for the case-details container before proceeding.
        """
        # Check if this is a real case page or a soft-404.
        # Speculation may extend beyond actual case numbers.
        containers = page.query_xpath(
            "//div[@class='case-details']",
            "case details container",
            min_count=0,
            max_count=1,
        )
        if not containers:
            return

        docket = (
            page.query_xpath(
                "//*[@id='docket']", "docket", min_count=1, max_count=1
            )[0]
            .text_content()
            .strip()
        )
        case_name = (
            page.query_xpath(
                "//h2", "case name heading", min_count=1, max_count=1
            )[0]
            .text_content()
            .strip()
        )

        yield ParsedData(
            CaseData.raw(
                request_url=response.url,
                docket=docket,
                case_name=case_name,
                plaintiff=page.query_xpath(
                    "//*[@id='plaintiff']",
                    "plaintiff",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                defendant=page.query_xpath(
                    "//*[@id='defendant']",
                    "defendant",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                date_filed=page.query_xpath(
                    "//*[@id='date-filed']",
                    "date filed",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                case_type=page.query_xpath(
                    "//*[@id='case-type']",
                    "case type",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                status=page.query_xpath(
                    "//*[@id='status']",
                    "status",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                judge=page.query_xpath(
                    "//*[@id='judge']",
                    "judge",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
                summary=page.query_xpath(
                    "//*[@id='summary']",
                    "summary",
                    min_count=1,
                    max_count=1,
                )[0]
                .text_content()
                .strip(),
            )
        )

        # Follow opinion link if present
        opinion_links = page.find_links(
            "//a[@class='opinion-link']",
            "opinion link",
            min_count=0,
            max_count=1,
        )
        if opinion_links:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=opinion_links[0].url,
                ),
                continuation=self.parse_opinion_detail,
                accumulated_data={
                    "docket": docket,
                    "case_name": case_name,
                },
            )

        # Follow oral-argument link if present
        audio_links = page.find_links(
            "//a[@class='audio-link']",
            "audio link",
            min_count=0,
            max_count=1,
        )
        if audio_links:
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=audio_links[0].url,
                ),
                continuation=self.parse_oral_argument_detail,
                accumulated_data={
                    "docket": docket,
                    "case_name": case_name,
                },
            )

    # ── Date search form submission & results parsing ────────────

    @step
    def submit_date_search(
        self,
        page: LxmlPageElement,
        accumulated_data: dict,
    ) -> Generator[ScraperYield, None, None]:
        """Find the date-search form and submit it with the date range."""
        form = page.find_form("//form[@id='date-search']", "date search form")
        request = form.submit(
            data={
                "from_date": accumulated_data["from_date"],
                "to_date": accumulated_data["to_date"],
            },
        )
        yield replace(request, continuation=self.parse_case_search_results)

    @step
    def parse_case_search_results(
        self,
        page: LxmlPageElement,
    ) -> Generator[ScraperYield, None, None]:
        """Parse the search results table and follow each case link."""
        rows = page.query_xpath(
            "//tr[@class='case-row']",
            "case search result rows",
            min_count=0,
        )
        for row in rows:
            links = row.find_links(
                ".//a[@class='case-link']",
                "case detail link",
                min_count=1,
                max_count=1,
            )
            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=links[0].url,
                ),
                continuation=self.parse_case_detail,
            )

    # ── Opinion parsing & archiving ─────────────────────────────

    @step
    def parse_opinion_detail(
        self,
        page: LxmlPageElement,
        response: Response,
        accumulated_data: dict,
    ) -> Generator[ScraperYield, None, None]:
        """Parse an opinion detail page and archive the illustration."""
        image_links = page.find_links(
            "//a[@class='opinion-image-link']",
            "opinion image link",
            min_count=1,
            max_count=1,
        )
        image_url = image_links[0].url

        yield EstimateData(
            expected_types=(OpinionData,),
            min_count=1,
            max_count=1,
        )

        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=image_url,
            ),
            continuation=self.archive_image,
            archive=True,
            expected_type="image",
            accumulated_data={
                **accumulated_data,
                "image_url": image_url,
            },
        )

    @step
    def archive_image(
        self,
        response: Response,
        accumulated_data: dict,
        local_filepath: str | None,
    ) -> Generator[ScraperYield, None, None]:
        """Handle an archived opinion illustration."""
        yield ParsedData(
            OpinionData.raw(
                request_url=response.url,
                docket=accumulated_data["docket"],
                case_name=accumulated_data["case_name"],
                image_url=accumulated_data["image_url"],
                local_path=local_filepath,
            )
        )

    # ── Oral argument parsing & archiving ───────────────────────

    @step
    def parse_oral_arguments_list(
        self,
        page: LxmlPageElement,
    ) -> Generator[ScraperYield, None, None]:
        """Parse the oral-arguments list page."""
        rows = page.query_xpath(
            "//tr[@class='oral-arg-row']",
            "oral argument rows",
            min_count=1,
        )

        yield EstimateData(
            expected_types=(OralArgumentData,),
            min_count=len(rows),
        )

        for row in rows:
            links = row.find_links(
                ".//a",
                "oral argument detail link",
                min_count=1,
                max_count=1,
            )
            # Extract docket from link text and case name from second cell
            docket = links[0].text.strip()
            cells = row.query_xpath(
                ".//td",
                "table cells",
                min_count=2,
            )
            case_name = cells[1].text_content().strip()

            yield Request(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=links[0].url,
                ),
                continuation=self.parse_oral_argument_detail,
                accumulated_data={
                    "docket": docket,
                    "case_name": case_name,
                },
            )

    @step
    def parse_oral_argument_detail(
        self,
        page: LxmlPageElement,
        response: Response,
        accumulated_data: dict,
    ) -> Generator[ScraperYield, None, None]:
        """Parse an oral-argument detail page and archive the audio."""
        audio_links = page.find_links(
            "//a[@class='audio-download-link']",
            "audio download link",
            min_count=1,
            max_count=1,
        )
        audio_url = audio_links[0].url

        yield Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=audio_url,
            ),
            continuation=self.archive_audio,
            archive=True,
            expected_type="audio",
            accumulated_data={
                **accumulated_data,
                "audio_url": audio_url,
            },
        )

    @step
    def archive_audio(
        self,
        response: Response,
        accumulated_data: dict,
        local_filepath: str | None,
    ) -> Generator[ScraperYield, None, None]:
        """Handle an archived oral-argument WAV file."""
        yield ParsedData(
            OralArgumentData.raw(
                request_url=response.url,
                docket=accumulated_data["docket"],
                case_name=accumulated_data["case_name"],
                audio_url=accumulated_data["audio_url"],
                local_path=local_filepath,
            )
        )

    # ── Justice JSON parsing ────────────────────────────────────

    @step
    def parse_justices_json(
        self,
        json_content: list,
    ) -> Generator[ScraperYield, None, None]:
        """Parse the ``/api/justices`` JSON array."""
        yield EstimateData(
            expected_types=(JusticeData,),
            min_count=len(json_content),
            max_count=len(json_content),
        )

        for j in json_content:
            yield ParsedData(
                JusticeData.raw(
                    request_url="",
                    name=j["name"],
                    insect_species=j["insect_species"],
                    title=j["title"],
                    appointed_date=j["appointed_date"],
                    bio=j["bio"],
                    image_url=j["image_url"],
                )
            )
