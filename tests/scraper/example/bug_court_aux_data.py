"""Bug Civil Court scraper demonstrating aux_data for navigation metadata.

This Step 6 scraper demonstrates how aux_data flows through request chains
to carry navigation metadata like session tokens that are needed for making
requests but aren't part of the case data itself.

The scraper:
1. Scrapes case list page, extracting hidden session_token field
2. Stores token in aux_data (not accumulated_data - it's not case data)
3. Navigates to case detail page with aux_data
4. Downloads PDF opinion using session token from aux_data in request header
5. Yields final data with both case info and PDF file path

This demonstrates the difference between:
- accumulated_data: "What case data have we collected?"
- aux_data: "What do we need to make the next request work?"
"""

from collections.abc import Generator

from lxml import html

from kent.common.decorators import entry
from kent.data_types import (
    ArchiveRequest,
    ArchiveResponse,
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
    ParsedData,
    Response,
    ScraperYield,
)


def _get_text(element, xpath: str) -> str:
    """Extract text content from an xpath query.

    Args:
        element: The lxml element to query.
        xpath: The XPath expression.

    Returns:
        The text content, or empty string if not found.
    """
    results = element.xpath(xpath)
    if results:
        return results[0].text_content().strip()
    return ""


def _get_text_by_id(tree, element_id: str) -> str:
    """Extract text content from an element by its ID.

    Args:
        tree: The lxml tree to query.
        element_id: The ID of the element.

    Returns:
        The text content, or empty string if not found.
    """
    return _get_text(tree, f"//*[@id='{element_id}']")


def _get_attr_by_id(tree, element_id: str, attr: str) -> str:
    """Extract an attribute value from an element by its ID.

    Args:
        tree: The lxml tree to query.
        element_id: The ID of the element.
        attr: The attribute name to extract.

    Returns:
        The attribute value, or empty string if not found.
    """
    elements = tree.xpath(f"//*[@id='{element_id}']")
    if elements:
        return elements[0].get(attr, "")
    return ""


class BugCourtScraperWithAuxData(BaseScraper[dict]):
    """Scraper for Bug Civil Court demonstrating aux_data.

    This Step 6 implementation demonstrates:
    - Using aux_data to carry session tokens through request chains
    - Extracting hidden form fields for navigation metadata
    - Using aux_data in HTTP request headers
    - Contrast between accumulated_data (case data) and aux_data (navigation metadata)

    The scraper visits three types of pages:
    1. List page (/cases) - extracts session token (aux_data) and docket (accumulated_data)
    2. Detail page (/cases/{docket}) - uses token from aux_data, extracts case data
    3. PDF download (/opinions/{docket}.pdf) - requires token in header from aux_data
    """

    BASE_URL = "http://127.0.0.1"

    @entry(dict)
    def get_entry(self) -> Generator[NavigatingRequest, None, None]:
        """Create the initial request to start scraping."""
        yield NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"{self.BASE_URL}/cases",
            ),
            continuation="parse_list",
        )

    def parse_list(
        self, response: Response
    ) -> Generator[ScraperYield[dict], None, None]:
        """Parse the case list page and extract session token + case data.

        This method demonstrates the key difference between aux_data and accumulated_data:
        - session_token goes in aux_data (navigation metadata, not case data)
        - docket goes in accumulated_data (case data we're collecting)

        Args:
            response: The Response from fetching the list page.

        Yields:
            NavigatingRequest for each case with both aux_data and accumulated_data.
        """
        tree = html.fromstring(response.text)

        # Extract the session token from hidden field
        # This is navigation metadata - needed for requests but not case data
        session_token = _get_attr_by_id(tree, "session-token", "value")

        # Find all case rows
        case_rows = tree.xpath("//tr[@class='case-row']")

        for row in case_rows:
            docket = _get_text(row, ".//td[@class='docket']")
            case_name = _get_text(row, ".//td[@class='case-name']")

            if docket:
                # Navigate to detail page with both types of data:
                # - aux_data: session token (needed for PDF download later)
                # - accumulated_data: case data (docket, case_name)
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"/cases/{docket}",
                    ),
                    continuation="parse_detail",
                    aux_data={"session_token": session_token},
                    accumulated_data={
                        "docket": docket,
                        "case_name": case_name,
                    },
                )

    def parse_detail(
        self, response: Response
    ) -> Generator[ScraperYield[dict], None, None]:
        """Parse detail page and check for PDF download.

        This method demonstrates using aux_data to access the session token
        while enriching accumulated_data with case details.

        Args:
            response: The Response from fetching the detail page.

        Yields:
            ArchiveRequest for PDF with session token in headers.
        """
        tree = html.fromstring(response.text)

        # Get accumulated_data and enrich it
        data = response.request.accumulated_data.copy()
        data["plaintiff"] = _get_text_by_id(tree, "plaintiff")
        data["defendant"] = _get_text_by_id(tree, "defendant")
        data["judge"] = _get_text_by_id(tree, "judge")
        data["date_filed"] = _get_text_by_id(tree, "date-filed")

        # Get aux_data for the session token
        aux = response.request.aux_data.copy()

        # Check for opinion PDF link
        opinion_links = tree.xpath('//a[contains(@href, "/opinions/")]/@href')
        if opinion_links:
            # Create headers with session token from aux_data
            headers = {"X-Session-Token": aux["session_token"]}

            # Download and archive the PDF
            # Pass both accumulated_data (case info) and aux_data (session token)
            yield ArchiveRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=opinion_links[0],
                    headers=headers,  # Token from aux_data!
                ),
                continuation="archive_opinion",
                expected_type="pdf",
                accumulated_data=data,  # Case data flows through
                aux_data=aux,  # Token flows through (though not needed after this)
            )
        else:
            # No PDF, just yield the case data
            yield ParsedData(data)

    def archive_opinion(
        self, response: ArchiveResponse
    ) -> Generator[ScraperYield[dict], None, None]:
        """Process archived opinion PDF and yield complete case data.

        Args:
            response: The ArchiveResponse from downloading the PDF.

        Yields:
            ParsedData with case information including the PDF file path.
        """
        # Get accumulated_data (case info from previous pages)
        data = response.request.accumulated_data.copy()

        # Add the PDF file path
        data["opinion_file"] = response.file_url
        data["download_url"] = response.url

        # Yield the complete case data
        # Note: We don't need to include aux_data in the final output -
        # it was just navigation metadata to make requests work
        yield ParsedData(data)
