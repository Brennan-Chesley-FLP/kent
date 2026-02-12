Step 3: NonNavigatingRequest - API Calls
==========================================

In Step 2, our scraper navigated from a list page to detail pages using
NavigatingRequest. Every request changed the driver's notion of "where we are"
(current_location).

But what if we want to fetch supplementary data from an API without navigating
away from the current page? This is where **NonNavigatingRequest** comes in.

This step introduces BaseRequest, NonNavigatingRequest, and current_location
tracking to distinguish between navigation and data fetching.


Overview
--------

In this step, we introduce:

1. **BaseRequest** - Base class for all request types with shared URL resolution
2. **NonNavigatingRequest** - Fetches data without updating current_location
3. **NavigatingRequest refactored** - Now inherits from BaseRequest
4. **current_location tracking** - Driver tracks "where we are" in the site
5. **URL resolution** - Both request types use urljoin for relative URLs


Why Two Request Types?
-----------------------

Consider this scenario: You're on a case detail page at ``/cases/BCC-2024-001``
and want to fetch additional metadata from ``/api/cases/BCC-2024-001``.

With only NavigatingRequest:

.. code-block:: python

    # We're at: /cases/BCC-2024-001
    yield NavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="/api/cases/BCC-2024-001",
        ),
        continuation="parse_api",
    )
    # Now current_location is: /api/cases/BCC-2024-001

    # If we yield a relative URL now, it resolves against the API endpoint!
    yield NavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="documents",
        ),
        continuation="parse_docs",
    )
    # Resolves to: /api/cases/documents (wrong!)

With NonNavigatingRequest, current_location stays put:

.. code-block:: python

    # We're at: /cases/BCC-2024-001
    yield NonNavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="/api/cases/BCC-2024-001",
        ),
        continuation="parse_api",
    )
    # current_location is still: /cases/BCC-2024-001

    # Relative URLs still resolve correctly
    yield NavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="documents",
        ),
        continuation="parse_docs",
    )
    # Resolves to: /cases/documents (correct!)


The Types
---------

**BaseRequest** provides shared functionality:

.. code-block:: python

    @dataclass(frozen=True)
    class BaseRequest:
        request: HTTPRequestParams  # All HTTP parameters bundled
        continuation: str
        current_location: str = ""
        previous_requests: list[BaseRequest] = field(default_factory=list)

        def resolve_url(self, current_location: str) -> str:
            """Resolve URL against current_location using urljoin.

            Also normalizes URL encoding (decode-then-encode) to prevent
            double-encoding issues.
            """
            # Normalize URL encoding
            parsed = urlparse(self.request.url)
            decoded_path = unquote(parsed.path)
            encoded_path = quote(decoded_path, safe="/")
            decoded_query = unquote(parsed.query)
            encoded_query = quote(decoded_query, safe="=&")

            reencoded_url = urlunparse(
                (parsed.scheme, parsed.netloc, encoded_path,
                 parsed.params, encoded_query, parsed.fragment)
            )
            return urljoin(current_location, reencoded_url)

        def resolve_request_from(self, context: Response | BaseRequest):
            """Helper method to extract common resolution logic.

            Returns a tuple of (HTTPRequestParams, resolved_location, parent_request)
            that subclasses use to construct their specific request type.
            """
            match context:
                case Response():
                    resolved_location = context.url
                    parent_request = context.request
                case BaseRequest():
                    resolved_location = context.current_location
                    parent_request = context

            return [
                HTTPRequestParams(
                    url=self.resolve_url(resolved_location),
                    method=self.request.method,
                    headers=self.request.headers,
                    data=self.request.data,
                ),
                resolved_location,
                parent_request,
            ]

**NavigatingRequest** updates current_location to the response URL:

.. code-block:: python

    @dataclass(frozen=True)
    class NavigatingRequest(BaseRequest):
        """Navigate to a new page, updating current_location."""

        def resolve_from(
            self, context: Response | NonNavigatingRequest
        ) -> NavigatingRequest:
            """Resolve from Response (after NavigatingRequest) or NonNavigatingRequest.

            Uses the helper method to extract common logic, then constructs
            a new NavigatingRequest with the resolved values.
            """
            request, location, parent = self.resolve_request_from(context)
            return NavigatingRequest(
                request=request,
                continuation=self.continuation,
                current_location=location,
                previous_requests=parent.previous_requests + [parent],
            )

**NonNavigatingRequest** preserves current_location from context:

.. code-block:: python

    @dataclass(frozen=True)
    class NonNavigatingRequest(BaseRequest):
        """Fetch data without navigating (e.g., API calls)."""

        def resolve_from(
            self, context: Response | NonNavigatingRequest
        ) -> NonNavigatingRequest:
            """Resolve from Response or NonNavigatingRequest.

            Uses the helper method to extract common logic, then constructs
            a new NonNavigatingRequest. The current_location is preserved
            from the context (response URL or originating request's location).
            """
            request, location, parent = self.resolve_request_from(context)
            return NonNavigatingRequest(
                request=request,
                continuation=self.continuation,
                current_location=location,
                previous_requests=parent.previous_requests + [parent],
            )


current_location Tracking
--------------------------

Each request carries its own ``current_location`` and request ancestry. The driver
passes the appropriate context when enqueueing requests:

.. code-block:: python

    @dataclass(frozen=True)
    class BaseRequest:
        request: HTTPRequestParams  # All HTTP parameters bundled together
        continuation: str
        current_location: str = ""  # Each request tracks its own location
        previous_requests: list["BaseRequest"] = field(default_factory=list)

The driver's ``run()`` method processes requests without tracking location state:

.. code-block:: python

    class SyncDriver:
        def __init__(self, scraper):
            self.scraper = scraper
            self.results = []
            self.request_queue = []

        def run(self):
            entry_request = self.scraper.get_entry()
            self.request_queue = [entry_request]

            while self.request_queue:
                request = self.request_queue.pop(0)
                response = self.resolve_request(request)

                continuation_method = self.scraper.get_continuation(request.continuation)
                for item in continuation_method(response):
                    match item:
                        case ParsedData():
                            self.results.append(item.unwrap())
                        case NavigatingRequest():
                            self.enqueue_request(item, response)  # Pass Response
                        case NonNavigatingRequest():
                            self.enqueue_request(item, request)  # Pass originating request
                        # ...

When enqueueing new requests, the driver passes the appropriate context:

.. code-block:: python

    def enqueue_request(
        self, new_request: BaseRequest, context: Response | BaseRequest
    ) -> None:
        """Enqueue a new request, resolving it from the given context.

        For NavigatingRequest yields: context is the Response
        For NonNavigatingRequest yields: context is the originating request
        """
        resolved_request = new_request.resolve_from(context)  # type: ignore
        self.request_queue.append(resolved_request)


Request Ancestry Tracking
--------------------------

Each request maintains a chain of previous requests via ``previous_requests``. This
enables debugging, and state reconstruction:

.. code-block:: python

    # Entry request has no ancestors
    entry = NavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="http://example.com/cases",
        ),
        continuation="parse_list",
    )
    assert entry.current_location == ""
    assert len(entry.previous_requests) == 0

    # After resolving from a Response, ancestry is tracked
    response = Response(url="http://example.com/cases", request=entry, ...)
    detail_request = NavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="/cases/001",
        ),
        continuation="parse_detail",
    )
    resolved = detail_request.resolve_from(response)

    assert resolved.current_location == "http://example.com/cases"
    assert len(resolved.previous_requests) == 1
    assert resolved.previous_requests[0] is entry

    # Ancestry grows with each request
    detail_response = Response(url="http://example.com/cases/001", request=resolved, ...)
    api_request = NonNavigatingRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url="/api/cases/001",
        ),
        continuation="parse_api",
    )
    resolved_api = api_request.resolve_from(detail_response)

    assert len(resolved_api.previous_requests) == 2
    assert resolved_api.previous_requests[0] is entry
    assert resolved_api.previous_requests[1] is resolved

This ancestry chain is useful for:

- **Debugging**: Trace the path that led to a specific request
- **State reconstruction**: Rebuild the scraper state from a serialized request
- **Error reporting**: Show the full request chain when errors occur
- **Analytics**: Understand scraper behavior and navigation patterns


URL Resolution
--------------

Both request types use ``urllib.parse.urljoin`` for URL resolution:

.. code-block:: python

    # Absolute URLs are returned unchanged
    urljoin("http://example.com/cases", "http://other.com/api")
    # => "http://other.com/api"

    # Relative URLs resolve against the base
    urljoin("http://example.com/cases/BCC-2024-001", "/api/cases/BCC-2024-001")
    # => "http://example.com/api/cases/BCC-2024-001"

    urljoin("http://example.com/cases/", "BCC-2024-001")
    # => "http://example.com/cases/BCC-2024-001"



Data Flow
---------

.. mermaid::

    sequenceDiagram
        participant D as Driver
        participant S as Scraper
        participant H as HTTP Client

        D->>H: GET /cases
        H-->>D: Response (list HTML)
        Note over D: current_location = /cases
        D->>S: parse_list(response)
        S-->>D: yield NavigatingRequest(/cases/BCC-2024-001)

        D->>H: GET /cases/BCC-2024-001
        H-->>D: Response (detail HTML)
        Note over D: current_location = /cases/BCC-2024-001
        D->>S: parse_detail(response)
        S-->>D: yield NonNavigatingRequest(/api/cases/BCC-2024-001)

        D->>H: GET /api/cases/BCC-2024-001
        H-->>D: Response (JSON)
        Note over D: current_location still /cases/BCC-2024-001
        D->>S: parse_api(response)
        S-->>D: yield ParsedData({...})

        D-->>D: Return all ParsedData


Example: Bug Court Scraper with API
------------------------------------

Here's the complete Step 3 scraper:

.. code-block:: python
    :caption: bug_court.py - BugCourtScraperWithAPI class

    """Bug Civil Court scraper with API metadata."""

    import json
    from collections.abc import Generator

    from lxml import html

    from kent.data_types import (
        BaseScraper,
        HttpMethod,
        HTTPRequestParams,
        NavigatingRequest,
        NonNavigatingRequest,
        ParsedData,
        Response,
        ScraperYield,
    )


    class BugCourtScraperWithAPI(BaseScraper[dict]):
        """Scraper that combines HTML pages with JSON API data.

        This demonstrates:
        - NavigatingRequest updates current_location
        - NonNavigatingRequest keeps current_location unchanged
        - Fetching supplementary data from an API
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
            """Parse the case list and navigate to detail pages."""
            tree = html.fromstring(response.text)
            case_rows = tree.xpath("//tr[@class='case-row']")

            for row in case_rows:
                docket = _get_text(row, ".//td[@class='docket']")
                if docket:
                    # Navigate to detail page
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET,
                            url=f"/cases/{docket}",
                        ),
                        continuation="parse_detail",
                    )

        def parse_detail(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse detail page and fetch API metadata without navigating."""
            tree = html.fromstring(response.text)
            docket = _get_text_by_id(tree, "docket")

            # Fetch API metadata WITHOUT navigating away
            # current_location stays at /cases/{docket}
            yield NonNavigatingRequest(
                request=HTTPRequestParams(
                    method=HttpMethod.GET,
                    url=f"/api/cases/{docket}",
                ),
                continuation="parse_api",
            )

        def parse_api(
            self, response: Response
        ) -> Generator[ScraperYield[dict], None, None]:
            """Parse JSON API response and yield complete data."""
            data = json.loads(response.text)

            yield ParsedData({
                "docket": data["docket"],
                "case_name": data["case_name"],
                "plaintiff": data["plaintiff"],
                "defendant": data["defendant"],
                "date_filed": data["date_filed"],
                "case_type": data["case_type"],
                "status": data["status"],
                "judge": data["judge"],
                "summary": data["summary"],
                # Additional metadata from API
                "api_metadata": data["api_metadata"],
            })


    def _get_text(element, xpath: str) -> str:
        results = element.xpath(xpath)
        if results:
            return results[0].text_content().strip()
        return ""


    def _get_text_by_id(tree, element_id: str) -> str:
        return _get_text(tree, f"//*[@id='{element_id}']")


The Driver Updates
------------------

The driver's ``run()`` method processes requests and passes appropriate context:

.. code-block:: python
    :caption: sync_driver.py - SyncDriver.run() method

    def run(self) -> list[ScraperReturnDatatype]:
        """Run the scraper starting from the scraper's entry point."""
        self.results = []
        entry_request = self.scraper.get_entry()
        self.request_queue = [entry_request]

        while self.request_queue:
            request: BaseRequest = self.request_queue.pop(0)
            response: Response = self.resolve_request(request)

            continuation_method = self.scraper.get_continuation(request.continuation)
            for item in continuation_method(response):
                match item:
                    case ParsedData():
                        self.results.append(item.unwrap())
                    case NavigatingRequest():
                        self.enqueue_request(item, response)  # Pass Response
                    case NonNavigatingRequest():
                        self.enqueue_request(item, request)  # Pass originating request
                    case None:
                        pass
                    case _:
                        assert_never(item)

        return self.results

    def enqueue_request(
        self, new_request: BaseRequest, context: Response | BaseRequest
    ) -> None:
        """Enqueue a new request, resolving it from the given context.

        For NavigatingRequest yields: context is the Response
        For NonNavigatingRequest yields: context is the originating request
        """
        resolved_request = new_request.resolve_from(context)  # type: ignore
        self.request_queue.append(resolved_request)

Recap

1. **No driver state**: The driver doesn't track current_location, we can safely make this concurrent
2. **Request carries state**: Each request has its own current_location and previous_requests
3. **Context-aware resolution**: NavigatingRequest gets Response, NonNavigatingRequest gets originating request
4. **Request ancestry**: The previous_requests list enables debugging and state reconstruction


Example: Using the Driver
--------------------------

.. code-block:: python

    from kent.driver.sync_driver import SyncDriver
    from tests.scraper_driver.scraper.example.bug_court import BugCourtScraperWithAPI

    # Create scraper and driver
    scraper = BugCourtScraperWithAPI()
    driver = SyncDriver(scraper)

    # Run the scraper
    results = driver.run()

    # Results now include API metadata
    for case in results:
        print(f"{case['docket']}: {case['case_name']}")
        print(f"  Jurisdiction: {case['api_metadata']['jurisdiction']}")


What's Next
-----------

In :doc:`04_archive_request`, we introduce **ArchiveRequest** - for
downloading and archiving files like PDFs and MP3s, with special handling
for binary content and local storage.
