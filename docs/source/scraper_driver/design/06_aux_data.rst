Step 6: Auxiliary Data - Navigation Metadata
============================================

In Step 5, we introduced accumulated_data for collecting case information across
multiple pages. But what about data that helps us navigate but isn't part of the
case data itself? This is where **aux_data** comes in.

This step introduces aux_data for navigation metadata like session tokens, API keys,
and hidden form fields that are needed to make requests work but shouldn't appear
in the final scraped data.


Overview
--------

In this step, we introduce:

1. **aux_data** - Field on BaseRequest for navigation metadata (tokens, session data)
2. **Deep copy semantics** - Same protection as accumulated_data
3. **Request chain propagation** - Data flows automatically through resolve_from
4. **Contrast with accumulated_data** - Clear separation of concerns
5. **No driver changes** - Data flows purely through Requests


Why Auxiliary Data?
-------------------

Consider this scenario: A court website requires a session token to download PDFs.
The token is in a hidden form field on the list page. You need to:

1. Extract the session token from the list page
2. Carry it to the detail page (without it being case data)
3. Use it in the HTTP headers when downloading the PDF
4. **NOT** include it in the final scraped data (it's not case information)

Without aux_data, you might be tempted to put it in accumulated_data:

.. code-block:: python

    # BAD: Mixing navigation metadata with case data
    yield NavigatingRequest(
        url=detail_url,
        continuation="parse_detail",
        accumulated_data={
            "case_name": "Ant v. Bee",  # Case data
            "session_token": "abc123",  # Navigation metadata - doesn't belong here!
        }
    )

    def parse_detail(self, response):
        data = response.request.accumulated_data.copy()
        # Now we have to remember to delete the token before yielding
        session_token = data.pop("session_token")  # Awkward!
        yield ParsedData(data)

With aux_data, navigation metadata stays separate:

.. code-block:: python

    # GOOD: Separate concerns
    yield NavigatingRequest(
        url=detail_url,
        continuation="parse_detail",
        accumulated_data={"case_name": "Ant v. Bee"},  # Case data only
        aux_data={"session_token": "abc123"},  # Navigation metadata only
    )

    def parse_detail(self, response):
        data = response.request.accumulated_data.copy()  # Case data
        token = response.request.aux_data["session_token"]  # Navigation data
        # Use token for headers, yield only case data
        yield ParsedData(data)


The aux_data Field
------------------

BaseRequest gets a new field alongside accumulated_data:

.. code-block:: python

    @dataclass(frozen=True)
    class BaseRequest:
        """Base class for all request types.

        Attributes:
            request: The HTTP request parameters.
            continuation: Name of the continuation method to call.
            current_location: The current URL (for relative URL resolution).
            previous_requests: Ancestry chain of requests.
            accumulated_data: Data collected across the request chain.
            aux_data: Navigation metadata needed for requests.
        """
        request: HTTPRequestParams
        continuation: str
        current_location: str = ""
        previous_requests: list[BaseRequest] = field(default_factory=list)
        accumulated_data: dict[str, Any] = field(default_factory=dict)
        aux_data: dict[str, Any] = field(default_factory=dict)

        def __post_init__(self) -> None:
            """Deep copy both accumulated_data and aux_data."""
            object.__setattr__(
                self, "accumulated_data", deepcopy(self.accumulated_data)
            )
            object.__setattr__(
                self, "aux_data", deepcopy(self.aux_data)
            )


Deep Copy Semantics
-------------------

Like accumulated_data, aux_data is deep-copied in ``__post_init__`` to prevent
sibling request contamination.

.. code-block:: python

    # Deep copy prevents mutation bugs
    shared_aux = {"session_token": "abc123"}

    request1 = NavigatingRequest(
        url="/case1",
        continuation="parse",
        aux_data=shared_aux
    )
    # __post_init__ deep copies shared_aux

    request2 = NavigatingRequest(
        url="/case2",
        continuation="parse",
        aux_data=shared_aux
    )
    # __post_init__ deep copies shared_aux again

    # Mutations don't leak
    shared_aux["session_token"] = "modified"
    assert request1.aux_data["session_token"] == "abc123"  # ✓
    assert request2.aux_data["session_token"] == "abc123"  # ✓


Contrast: accumulated_data vs aux_data
---------------------------------------

The key difference is **intent and usage**. By separating these out, we can do partial validation,
scrape pruning, and improve debuggability.

**Example contrast**:

.. code-block:: python

    # List page: extract both types of data
    session_token = extract_token(html)  # aux_data
    case_name = extract_case_name(html)  # accumulated_data

    yield NavigatingRequest(
        url=detail_url,
        continuation="parse_detail",
        accumulated_data={"case_name": case_name},  # Will be in final output
        aux_data={"session_token": session_token},  # Won't be in final output
    )

    # Detail page: use aux_data for requests, enrich accumulated_data
    def parse_detail(self, response):
        data = response.request.accumulated_data.copy()  # Case data
        data["judge"] = extract_judge(html)  # More case data

        token = response.request.aux_data["session_token"]  # Navigation data
        headers = {"X-Session-Token": token}  # Use for download

        yield ArchiveRequest(
            url=pdf_url,
            headers=headers,  # Token used here
            continuation="archive_pdf",
            accumulated_data=data,  # Case data flows
            aux_data=response.request.aux_data,  # Token flows (but not used after this)
        )

    # Archive: yield only case data
    def archive_pdf(self, response: ArchiveResponse):
        data = response.request.accumulated_data.copy()
        data["pdf_file"] = response.file_url
        yield ParsedData(data)  # No aux_data in final output!


Data Flow Diagram
-----------------

.. mermaid::

    sequenceDiagram
        participant S as Scraper
        participant D as Driver
        participant H as HTTP Client

        Note over D: Entry request

        D->>H: GET /cases
        H-->>D: Response (HTML with hidden token)
        D->>S: parse_list(response)
        Note over S: Extract session_token → aux_data<br/>Extract case_name → accumulated_data
        S-->>D: yield NavigatingRequest(<br/>  url=/cases/BCC-001,<br/>  accumulated_data={case_name: "Ant v. Bee"},<br/>  aux_data={session_token: "abc123"})

        Note over D: resolve_from propagates both dicts

        D->>H: GET /cases/BCC-001
        H-->>D: Response (detail HTML)
        D->>S: parse_detail(response)
        Note over S: Get token from aux_data<br/>Enrich accumulated_data<br/>Use token in headers
        S-->>D: yield ArchiveRequest(<br/>  url=/opinions/BCC-001.pdf,<br/>  headers={X-Session-Token: "abc123"},<br/>  accumulated_data={case_name, judge, docket},<br/>  aux_data={session_token: "abc123"})

        Note over D: resolve_from propagates both dicts

        D->>H: GET /opinions/BCC-001.pdf<br/>X-Session-Token: abc123
        H-->>D: ArchiveResponse (PDF content + file_url)
        D->>S: archive_opinion(response)
        Note over S: Get accumulated_data<br/>Add file_url<br/>Yield final data (no aux_data!)
        S-->>D: yield ParsedData({<br/>  case_name: "Ant v. Bee",<br/>  judge: "Judge Mantis",<br/>  docket: "BCC-001",<br/>  pdf_file: "/tmp/BCC-001.pdf"<br/>})

        D-->>D: Return all ParsedData


Example: Bug Court Scraper with aux_data
-----------------------------------------

Here's the complete Step 6 scraper:

.. code-block:: python
    :caption: bug_court_aux_data.py

    """Bug Civil Court scraper demonstrating aux_data."""

    from collections.abc import Generator
    from lxml import html

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


    class BugCourtScraperWithAuxData(BaseScraper[dict]):
        """Scraper demonstrating aux_data for navigation metadata.

        This demonstrates:
        - Using aux_data to carry session tokens
        - Extracting hidden form fields for navigation
        - Using aux_data in HTTP request headers
        - Contrast between accumulated_data and aux_data
        """

        BASE_URL = "http://bugcourt.example.com"

        def get_entry(self) -> NavigatingRequest:
            return NavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"{self.BASE_URL}/cases",
                ),
                continuation="parse_list",
            )

        def parse_list(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Extract session token (aux_data) and case data (accumulated_data)."""
            tree = html.fromstring(response.text)

            # Extract session token - navigation metadata
            session_token = tree.xpath('//*[@id="session-token"]/@value')[0]

            # Find all case rows
            case_rows = tree.xpath("//tr[@class='case-row']")

            for row in case_rows:
                docket = row.xpath(".//td[@class='docket']/text()")[0].strip()
                case_name = row.xpath(".//td[@class='case-name']/text()")[0].strip()

                # Separate concerns:
                # - aux_data: session token (needed for PDF download later)
                # - accumulated_data: case data (docket, case_name)
                yield NavigatingRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"/cases/{docket}",
                    ),
                    continuation="parse_detail",
                    aux_data={"session_token": session_token},
                    accumulated_data={"docket": docket, "case_name": case_name},
                )

        def parse_detail(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Use aux_data for headers, enrich accumulated_data."""
            tree = html.fromstring(response.text)

            # Get accumulated_data and enrich it
            data = response.request.accumulated_data.copy()
            data["plaintiff"] = tree.xpath('//*[@id="plaintiff"]/text()')[0].strip()
            data["defendant"] = tree.xpath('//*[@id="defendant"]/text()')[0].strip()
            data["judge"] = tree.xpath('//*[@id="judge"]/text()')[0].strip()

            # Get aux_data for the session token
            token = response.request.aux_data["session_token"]

            # Check for PDF
            opinion_links = tree.xpath('//a[contains(@href, "/opinions/")]/@href')
            if opinion_links:
                # Create headers with session token from aux_data
                headers = {"X-Session-Token": token}

                yield ArchiveRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=opinion_links[0],
                        headers=headers,  # Token from aux_data!
                    ),
                    continuation="archive_opinion",
                    expected_type="pdf",
                    accumulated_data=data,  # Case data flows
                    aux_data=response.request.aux_data,  # Token flows
                )
            else:
                yield ParsedData(data)

        def archive_opinion(
            self, response: ArchiveResponse
        ) -> Generator[ScraperYield[dict], None, None]:
            """Yield case data without aux_data."""
            # Get accumulated_data (case info)
            data = response.request.accumulated_data.copy()
            data["opinion_file"] = response.file_url

            # Yield case data only - no aux_data in final output
            yield ParsedData(data)

What's Next
-----------

In :doc:`07_callbacks`, we start thinking about making it easier for the
Driver to be customized for various use cases. We introduce the first hook
for handling ParsedData.
