===================================
Step 19: Step Decorators
===================================

The Sugar
-----------

Scraper methods need to:
- Parse different content types (HTML, JSON, text)
- Access request context (current request, previous request)
- Avoid boilerplate code for parsing and error handling


The Solution
------------

A single **@step** decorator that uses **argument inspection** to determine what to inject:

.. code-block:: python

    @step
    def parse_page(self, lxml_tree, response):
        # lxml_tree and response automatically injected!
        cases = lxml_tree.checked_xpath("//div[@class='case']", "cases")
        for case in cases:
            yield ParsedData(...)

    @step
    def parse_api(self, json_content, request):
        # json_content and request automatically injected!
        for item in json_content['items']:
            yield ParsedData(...)

Supported Parameter Names
--------------------------

The decorator inspects function signatures and injects values based on parameter names:

- **response**: The Response object
- **request**: The current BaseRequest
- **previous_request**: The parent request from the chain (None for entry request)
- **json_content**: Response content parsed as JSON
- **lxml_tree**: Response content parsed as CheckedHtmlElement
- **text**: Response content as string
- **accumulated_data**: From the Request object
- **aux_data**: From the Request object

Implementation Details
----------------------

Content Parsing
^^^^^^^^^^^^^^^

Content is parsed on-demand based on requested parameters:

- Only parse JSON if ``json_content`` is requested
- Only parse HTML if ``lxml_tree`` is requested
- Parsing errors raise ``ScraperAssumptionException``

Callable Continuations
^^^^^^^^^^^^^^^^^^^^^^

The decorator auto-resolves Callable continuations to function names:

.. code-block:: python

    @step
    def parse_first(self, response):
        # Can use Callable instead of string!
        yield NavigatingRequest(
            url="/next",
            continuation=self.parse_second  # Callable!
        )

    @step
    def parse_second(self, response):
        yield ParsedData(...)

Priority Metadata
^^^^^^^^^^^^^^^^^

Priority can be attached to steps:

.. code-block:: python

    @step(priority=5)  # Higher priority (lower number)
    def parse_urgent(self, response):
        yield ParsedData(...)

    @step(priority=9)  # Default priority
    def parse_normal(self, response):
        yield ParsedData(...)

Usage Examples
--------------

HTML Parsing
^^^^^^^^^^^^

.. code-block:: python

    class CourtScraper(BaseScraper[dict]):
        @step
        def parse_list(self, lxml_tree, response):
            cases = lxml_tree.checked_xpath("//tr[@class='case']", "cases")
            for case in cases:
                docket = case.checked_xpath(".//td[@class='docket']", "docket")[0]
                yield NavigatingRequest(
                    url=f"/cases/{docket.text_content()}",
                    continuation="parse_detail"
                )

        @step
        def parse_detail(self, lxml_tree, response):
            yield ParsedData(data={
                "docket": lxml_tree.checked_xpath("//h1", "title")[0].text,
                "summary": lxml_tree.checked_xpath("//div[@class='summary']", "summary")[0].text
            })

JSON API
^^^^^^^^

.. code-block:: python

    @step
    def parse_api(self, json_content, accumulated_data):
        case_id = accumulated_data['case_id']

        # Parse JSON
        for item in json_content['results']:
            yield ParsedData(data=item, case_id=case_id)

Next Steps
----------

In :doc:`17_search_and_standardization`, we introduce standardized metadata
on scrapers, ``@entry`` decorators with typed parameters, and the
``initial_seed()`` interface for configuring scraper filters.

