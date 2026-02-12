Step 4: ArchiveRequest - File Downloads
========================================

In Step 3, our scraper fetched JSON API data without navigating using
NonNavigatingRequest. We may also want to download and save files
like PDFs, MP3s, or images? This is where **ArchiveRequest** comes in.

This step introduces ArchiveRequest and ArchiveResponse for downloading
and archiving files to local storage.


Overview
--------

In this step, we introduce:

1. **ArchiveRequest** - Request to download and save a file
2. **ArchiveResponse** - Response with file_url field for local storage path
3. **File storage** - Driver manages local storage directory
4. **Filename extraction** - Extract filename from URL or generate based on type
5. **Binary content handling** - Save raw bytes to disk


Why Archive Files?
------------------

Consider this scenario: You're scraping court opinions that are available as
PDF downloads. You want to:

1. Download the PDF file
2. Save it to local storage
3. Include the local file path in your scraped data

Without ArchiveRequest, you'd have to:

.. code-block:: python

    # Fetch the PDF
    pdf_response = fetch_url(pdf_url)

    # Manually save it
    with open(f"/tmp/{docket}.pdf", "wb") as f:
        f.write(pdf_response.content)

    # Track the file path yourself
    yield ParsedData({"opinion_file": f"/tmp/{docket}.pdf"})

With ArchiveRequest, the driver handles all of this:

.. code-block:: python

    # Just yield the request
    yield ArchiveRequest(
        request=HTTPRequestParams(
            method=HttpMethod.GET,
            url=f"/opinions/{docket}.pdf",
        ),
        continuation="archive_opinion",
        expected_type="pdf",
    )

    # In your continuation method, file is already saved:
    def archive_opinion(self, response: ArchiveResponse):
        # response.file_url contains the local path
        yield ParsedData({"opinion_file": response.file_url})


The Types
---------

**ArchiveRequest** inherits from NonNavigatingRequest and requests a file download:

.. code-block:: python

    @dataclass(frozen=True)
    class ArchiveRequest(NonNavigatingRequest):
        """A request to download and archive a file.

        When a scraper yields an ArchiveRequest, the driver will:
        1. Fetch the URL (resolving relative URLs against current_location)
        2. Download the file content
        3. Save it to local storage
        4. Call the continuation method with an ArchiveResponse

        Like NonNavigatingRequest, ArchiveRequest preserves current_location -
        downloading a file doesn't change where you are in the scraper's navigation.

        Attributes:
            expected_type: Optional hint about the file type ("pdf", "audio", etc.).
        """

        expected_type: str | None = None

        def resolve_from(
            self, context: Response | NonNavigatingRequest
        ) -> ArchiveRequest:
            """Create a new request with URL resolved from a Response or NonNavigatingRequest.

            For ArchiveRequest (like NonNavigatingRequest):
            - If context is a Response, use the response's URL as current_location
            - If context is a NonNavigatingRequest, use its current_location
            - current_location stays unchanged (inherited from parent)
            """
            request, location, parent = self.resolve_request_from(context)
            return ArchiveRequest(
                request=request,
                continuation=self.continuation,
                current_location=location,
                previous_requests=parent.previous_requests + [parent],
                expected_type=self.expected_type,
            )

**ArchiveResponse** extends Response with a file_url field:

.. code-block:: python

    @dataclass
    class ArchiveResponse(Response):
        """HTTP response for an archived file.

        Extends Response with a file_url field that contains the local storage
        path where the file was saved. This allows scrapers to include the
        file location in their ParsedData output.

        Attributes:
            file_url: Local file system path where the downloaded file was stored.
        """

        file_url: str = ""


File Storage
------------

For now, the driver manages file storage through a ``storage_dir`` parameter:

.. code-block:: python

    class SyncDriver(Generic[ScraperReturnDatatype]):
        def __init__(
            self,
            scraper: BaseScraper[ScraperReturnDatatype],
            storage_dir: Path | None = None,
        ) -> None:
            """Initialize the driver.

            Args:
                scraper: Scraper instance with continuation methods.
                storage_dir: Directory for storing downloaded files.
                    If None, uses system temp directory.
            """
            self.scraper = scraper
            self.results: list[ScraperReturnDatatype] = []
            self.request_queue: list[BaseRequest] = []
            self.storage_dir = storage_dir or Path(gettempdir()) / "juriscraper_files"
            self.storage_dir.mkdir(parents=True, exist_ok=True)

The driver:

- Creates the storage directory if it doesn't exist
- Uses system temp directory by default (``/tmp/juriscraper_files`` on Unix)
- Allows custom storage directory for production use


Filename Extraction
-------------------

The driver's ``save_file()`` method extracts filenames from URLs or generates them:

.. code-block:: python

    def save_file(
        self, content: bytes, url: str, expected_type: str | None
    ) -> str:
        """Save downloaded file content to local storage.

        Args:
            content: The binary file content.
            url: The URL the file was downloaded from.
            expected_type: Optional hint about the file type.

        Returns:
            The local file path where the file was saved.
        """
        # Extract filename from URL or generate one
        parsed_url = urlparse(url)
        path_parts = Path(parsed_url.path).parts
        if path_parts:
            filename = path_parts[-1]
        else:
            # Generate a filename based on expected_type
            ext = {"pdf": ".pdf", "audio": ".mp3"}.get(expected_type or "", "")
            filename = f"download_{hash(url)}{ext}"

        file_path = self.storage_dir / filename
        file_path.write_bytes(content)
        return str(file_path)

Examples:

.. code-block:: python

    # URL: http://example.com/opinions/BCC-2024-001.pdf
    # Filename: BCC-2024-001.pdf

    # URL: http://example.com/download?id=123 (expected_type="pdf")
    # Filename: download_-1234567890.pdf  (hash-based)

    # URL: http://example.com/audio.php?case=BCC-2024-001 (expected_type="audio")
    # Filename: download_9876543210.mp3  (hash-based)


Driver Handling
---------------

The driver's ``run()`` method dispatches to the appropriate resolution method:

.. code-block:: python

    def run(self) -> list[ScraperReturnDatatype]:
        """Run the scraper starting from the scraper's entry point."""
        self.results = []
        entry_request = self.scraper.get_entry()
        self.request_queue = [entry_request]

        while self.request_queue:
            request: BaseRequest = self.request_queue.pop(0)
            # Dispatch to resolve_archive_request for ArchiveRequest
            response: Response = (
                self.resolve_archive_request(request)
                if isinstance(request, ArchiveRequest)
                else self.resolve_request(request)
            )

            continuation_method = self.scraper.get_continuation(request.continuation)
            for item in continuation_method(response):
                match item:
                    case ParsedData():
                        self.results.append(item.unwrap())
                    case NavigatingRequest():
                        self.enqueue_request(item, response)
                    case NonNavigatingRequest():
                        self.enqueue_request(item, request)
                    case ArchiveRequest():
                        self.enqueue_request(item, response)  # New case!
                    case None:
                        pass
                    case _:
                        assert_never(item)

        return self.results

The ``resolve_request()`` method handles standard HTTP requests:

.. code-block:: python

    def resolve_request(self, request: BaseRequest) -> Response:
        """Fetch a BaseRequest and return the Response."""
        http_params = request.request
        with httpx.Client() as client:
            http_response = client.request(
                method=http_params.method.value,
                url=http_params.url,
                headers=http_params.headers if http_params.headers else None,
                content=http_params.data
                if isinstance(http_params.data, bytes)
                else None,
                data=http_params.data
                if isinstance(http_params.data, dict)
                else None,
            )

        return Response(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=http_params.url,
            request=request,
        )

The ``resolve_archive_request()`` method reuses ``resolve_request()`` and adds file storage:

.. code-block:: python

    def resolve_archive_request(self, request: ArchiveRequest) -> ArchiveResponse:
        """Fetch an ArchiveRequest, download the file, and return an ArchiveResponse.

        This method reuses resolve_request() to fetch the file, then saves it
        to local storage and returns an ArchiveResponse with the file_url.
        """
        # Reuse the standard HTTP request logic
        http_response = self.resolve_request(request)

        # Save the file and get the local path
        file_url = self.save_file(
            content=http_response.content,
            url=request.request.url,
            expected_type=request.expected_type,
        )

        return ArchiveResponse(
            status_code=http_response.status_code,
            headers=http_response.headers,
            content=http_response.content,
            text=http_response.text,
            url=request.request.url,
            request=request,
            file_url=file_url,
        )


Data Flow
---------

.. mermaid::

    sequenceDiagram
        participant S as Scraper
        participant D as Driver
        participant H as HTTP Client
        participant F as Filesystem

        D->>H: GET /cases
        H-->>D: Response (list HTML)
        D->>S: parse_list(response)
        S-->>D: yield NavigatingRequest(/cases/BCC-2024-002)

        D->>H: GET /cases/BCC-2024-002
        H-->>D: Response (detail HTML)
        D->>S: parse_detail(response)
        S-->>D: yield ArchiveRequest(/opinions/BCC-2024-002.pdf)

        D->>H: GET /opinions/BCC-2024-002.pdf
        H-->>D: Response (PDF binary)
        D->>F: Write /tmp/juriscraper_files/BCC-2024-002.pdf
        F-->>D: File saved
        D->>S: archive_opinion(ArchiveResponse{file_url: /tmp/juriscraper_files/BCC-2024-002.pdf})
        S-->>D: yield ParsedData({opinion_file: /tmp/juriscraper_files/BCC-2024-002.pdf})

        D-->>D: Return all ParsedData


Example: Bug Court Scraper with Archive
----------------------------------------

Here's the complete Step 4 scraper:

.. code-block:: python
    :caption: bug_court.py - BugCourtScraperWithArchive class

    """Bug Civil Court scraper with file archiving."""

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


    class BugCourtScraperWithArchive(BaseScraper[dict]):
        """Scraper for the Bug Civil Court with file archiving.

        This demonstrates:
        - Using ArchiveRequest to download PDF opinions
        - Using ArchiveRequest to download MP3 oral arguments
        - ArchiveResponse provides file_url for local storage path
        - Combining archived file paths with case metadata
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
            """Parse detail page and check for downloadable files."""
            tree = html.fromstring(response.text)

            docket = _get_text_by_id(tree, "docket")
            case_name = _get_text(tree, "//h2")

            # Check for opinion PDF link
            opinion_links = tree.xpath('//a[contains(@href, "/opinions/")]/@href')
            if opinion_links:
                # Download and archive the PDF
                yield ArchiveRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=opinion_links[0],
                    ),
                    continuation="archive_opinion",
                    expected_type="pdf",
                )

            # Check for oral argument MP3 link
            oral_arg_links = tree.xpath(
                '//a[contains(@href, "/oral-arguments/")]/@href'
            )
            if oral_arg_links:
                # Download and archive the MP3
                yield ArchiveRequest(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=oral_arg_links[0],
                    ),
                    continuation="archive_oral_argument",
                    expected_type="audio",
                )

            # If there are no files to download, yield the data directly
            if not opinion_links and not oral_arg_links:
                yield ParsedData(
                    {
                        "docket": docket,
                        "case_name": case_name,
                        "plaintiff": _get_text_by_id(tree, "plaintiff"),
                        "defendant": _get_text_by_id(tree, "defendant"),
                        "date_filed": _get_text_by_id(tree, "date-filed"),
                        "case_type": _get_text_by_id(tree, "case-type"),
                        "status": _get_text_by_id(tree, "status"),
                        "judge": _get_text_by_id(tree, "judge"),
                        "summary": _get_text_by_id(tree, "summary"),
                    }
                )

        def archive_opinion(
            self, response: ArchiveResponse
        ) -> Generator[ScraperYield[dict], None, None]:
            """Process archived opinion PDF and yield case data.

            The ArchiveResponse includes file_url with the local storage path.
            """
            # The file_url contains the local path where the PDF was saved
            yield ParsedData(
                {
                    "docket": response.request.current_location.split("/")[-1],
                    "opinion_file": response.file_url,
                    "download_url": response.url,
                }
            )

        def archive_oral_argument(
            self, response: ArchiveResponse
        ) -> Generator[ScraperYield[dict], None, None]:
            """Process archived oral argument MP3 and yield case data.

            The ArchiveResponse includes file_url with the local storage path.
            """
            # The file_url contains the local path where the MP3 was saved
            yield ParsedData(
                {
                    "docket": response.request.current_location.split("/")[-1],
                    "oral_argument_file": response.file_url,
                    "download_url": response.url,
                }
            )


    def _get_text(element, xpath: str) -> str:
        results = element.xpath(xpath)
        if results:
            return results[0].text_content().strip()
        return ""


    def _get_text_by_id(tree, element_id: str) -> str:
        return _get_text(tree, f"//*[@id='{element_id}']")


Example: Using the Driver
--------------------------

.. code-block:: python

    from pathlib import Path

    from kent.driver.sync_driver import SyncDriver
    from tests.scraper_driver.scraper.example.bug_court import (
        BugCourtScraperWithArchive,
    )

    # Create scraper and driver with custom storage
    scraper = BugCourtScraperWithArchive()
    storage_dir = Path("/var/data/court_files")
    driver = SyncDriver(scraper, storage_dir=storage_dir)

    # Run the scraper
    results = driver.run()

    # Results include local file paths
    for case in results:
        if "opinion_file" in case:
            print(f"Opinion saved to: {case['opinion_file']}")
        if "oral_argument_file" in case:
            print(f"Oral argument saved to: {case['oral_argument_file']}")


Key Points
----------

1. **ArchiveRequest** - Inherits from BaseRequest, adds expected_type field
2. **ArchiveResponse** - Inherits from Response, adds file_url field
3. **Automatic file saving** - Driver handles all file I/O
4. **Filename extraction** - From URL path or generated from hash
5. **Storage management** - Driver creates and manages storage directory


What's Next
-----------

In :doc:`05_accumulated_data`, we will introduce accumulated data so that we
can collect information spread out across multiple pages.