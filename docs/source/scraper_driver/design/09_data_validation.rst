Step 9: Data Validation Errors
===============================

In Step 8, we introduced structural assumption errors for when HTML structure
changes. Now we address **data format assumptions** - what happens when scraped
data doesn't match the expected schema?

Scrapers extract data and consumers expect it in a specific format. When the
data doesn't match the expected schema (missing fields, wrong types, invalid
values), the scraper should segregate that result so we can handle is differently.

This step introduces **Pydantic model validation** and **DataFormatAssumptionException**
to validate scraped data and provide clear diagnostic information when data
format changes are detected.


Overview
--------

In this step, we introduce:

1. **DataFormatAssumptionException** - Exception for data schema mismatches
2. **Pydantic Models** - Schema definitions for scraped data
3. **DeferredValidation** - Delay validation until driver validates
4. **Driver-Side Validation** - Driver validates data and handles errors via callbacks


Exception: DataFormatAssumptionException
-----------------------------------------

The DataFormatAssumptionException is raised when scraped data doesn't match
the expected Pydantic model:

.. code-block:: python

    class DataFormatAssumptionException(ScraperAssumptionException):
        """Raised when scraped data doesn't match expected schema."""

        def __init__(
            self,
            errors: list[dict[str, Any]],
            failed_doc: dict[str, Any],
            model_name: str,
            request_url: str,
        ):
            self.errors = errors  # Pydantic validation errors
            self.failed_doc = failed_doc  # The data that failed
            self.model_name = model_name  # Model being validated against
            # ...

This exception includes:

- **errors** - List of Pydantic validation errors with field locations and messages
- **failed_doc** - The complete document that failed validation
- **model_name** - Name of the Pydantic model
- **request_url** - URL where the data was scraped from


Pydantic Models for Data
-------------------------

Define the expected schema using Pydantic models:

Basic Model
^^^^^^^^^^^

.. code-block:: python

    from datetime import date
    from pydantic import BaseModel, Field, HttpUrl

    class BugCourtCaseData(BaseModel):
        """Expected schema for Bug Court case data."""

        docket: str = Field(..., description="Docket number")
        case_name: str = Field(..., description="Full case name")
        plaintiff: str
        defendant: str
        date_filed: date  # Python date object
        case_type: str
        status: str
        judge: str
        court_reporter: str
        pdf_url: HttpUrl | None = None  # Optional

**Benefits:**

- Type checking (str, date, HttpUrl, etc.)
- Required vs optional fields
- Field descriptions for documentation
- Automatic validation

Deferred Validation in Scrapers
--------------------------------

Scrapers use the `.raw()` method to create DeferredValidation wrappers. The driver
will call `confirm()` to validate the data later. I'll be the first to admit that
the raw and confirm steps are a little rough, but it lets us have simpler static
type checking that does what we'd like without extraordinary measures.

.. code-block:: python

    from kent.common.data_models import ScrapedData

    class BugCourtCaseData(ScrapedData):
        """Data model inheriting from ScrapedData for deferred validation."""
        docket: str
        case_name: str
        # ... other fields

    def parse_detail(self, response: Response):
        tree = CheckedHtmlElement(html.fromstring(response.text), response.url)

        # Step 9: Yield deferred validation - driver will validate
        yield ParsedData(
            BugCourtCaseData.raw(
                request_url=response.url,
                docket=extract_docket(tree),
                case_name=extract_case_name(tree),
                plaintiff=extract_plaintiff(tree),
                defendant=extract_defendant(tree),
                date_filed=parse_date(extract_date(tree)),
                case_type=extract_type(tree),
                status=extract_status(tree),
                judge=extract_judge(tree),
                court_reporter=extract_reporter(tree),
            )
        )

**Benefits:**

- Scraper stays pure - no validation logic, just data extraction
- Driver handles all validation and error callbacks
- Consistent error handling across all scrapers


Error Messages
^^^^^^^^^^^^^^

When validation fails, you get detailed error information:

.. code-block:: text

    Data validation failed for model 'BugCourtCaseData': date_filed: field required
    URL: http://example.com/cases/BCC-2024-001
    Context:
      model: BugCourtCaseData
      error_count: 1
      errors: [{'loc': ('date_filed',), 'msg': 'field required', 'type': 'value_error.missing'}]
      failed_doc: {'docket': 'BCC-2024-001', 'case_name': 'Ant v. Grasshopper', ...}


Driver-Side Validation
----------------------

The driver automatically validates DeferredValidation objects when they are yielded.
Validation happens in the driver's request processing loop:

.. code-block:: python

    driver = SyncDriver(
        scraper=my_scraper,
        on_data=handle_valid_data,           # Receives validated Pydantic models
        on_invalid_data=handle_invalid_data,  # Receives DeferredValidation for invalid data
    )

    driver.run()

**Validation Flow:**

1. Scraper yields ParsedData containing DeferredValidation
2. Driver detects DeferredValidation and calls `confirm()`
3. If validation succeeds: validated model sent to `on_data` callback
4. If validation fails:

   - If `on_invalid_data` callback present: DeferredValidation sent to it
   - Elif `on_data` callback present: DeferredValidation sent to it (fallback)
   - Else: DataFormatAssumptionException propagates

**Example with callbacks:**

.. code-block:: python

    valid_results = []
    invalid_results = []

    def on_valid(data):
        # data is a validated BugCourtCaseData instance
        valid_results.append(data)

    def on_invalid(data):
        # data is a DeferredValidation instance
        logger.error(f"Invalid data: {data.raw_data}")
        invalid_results.append(data)

    driver = SyncDriver(
        scraper=scraper,
        on_data=on_valid,
        on_invalid_data=on_invalid,
    )

    driver.run()

**Default Logging Callback:**

The framework provides a default `log_and_validate_invalid_data` callback that logs
validation errors at the error level:

.. code-block:: python

    from kent.driver.sync_driver import (
        SyncDriver,
        log_and_validate_invalid_data,
    )

    driver = SyncDriver(
        scraper=scraper,
        on_invalid_data=log_and_validate_invalid_data,  # Logs errors with full context
    )
    driver.run()

This callback:

- Calls `confirm()` to trigger validation
- Catches `DataFormatAssumptionException`
- Logs error with model name, error count, and field-level details
- Includes structured logging with `extra` dict for monitoring systems


DeferredValidation Pattern
---------------------------

The DeferredValidation class wraps unvalidated data and delays validation
until the driver calls confirm():

Basic Usage
^^^^^^^^^^^

.. code-block:: python

    from kent.common.data_models import ScrapedData

    class CaseData(ScrapedData):
        docket: str
        case_name: str
        # ... other fields

    # In scraper - create deferred validation using .raw()
    deferred = CaseData.raw(
        request_url=response.url,
        docket="BCC-2024-001",
        case_name="Ant v. Grasshopper",
        # ... data fields
    )
    yield ParsedData(deferred)

    # In driver - validate when ready
    try:
        validated_case = deferred.confirm()  # Raises if invalid
        on_data(validated_case)
    except DataFormatAssumptionException as e:
        on_invalid_data(deferred)


Example: Bug Court Scraper with Validation
-------------------------------------------

Here's the complete Bug Court scraper with deferred data validation:

.. code-block:: python

    from datetime import date
    from pydantic import Field, HttpUrl
    from kent.common.data_models import ScrapedData

    # Data model defined in scraper (not imported)
    class BugCourtCaseData(ScrapedData):
        """Expected schema for Bug Court case data."""
        docket: str = Field(..., description="Docket number")
        case_name: str = Field(..., description="Full case name")
        plaintiff: str
        defendant: str
        date_filed: date
        case_type: str
        status: str
        judge: str
        court_reporter: str
        pdf_url: HttpUrl | None = None

    class BugCourtScraperWithValidation(BaseScraper[BugCourtCaseData]):
        """Scraper with Pydantic data validation (validated by driver)."""

        def parse_detail(self, response: Response):
            tree = CheckedHtmlElement(html.fromstring(response.text), response.url)

            # Validate structure (Step 8)
            tree.checked_xpath("//div[@class='case-details']", "case details", min_count=1)

            # Step 9: Yield deferred validation - driver validates
            yield ParsedData(
                BugCourtCaseData.raw(
                    request_url=response.url,
                    docket=get_text_by_id(tree, "docket"),
                    case_name=get_text(tree, "//h2"),
                    plaintiff=get_text_by_id(tree, "plaintiff"),
                    defendant=get_text_by_id(tree, "defendant"),
                    date_filed=parse_date(get_text_by_id(tree, "date-filed")),
                    case_type=get_text_by_id(tree, "case-type"),
                    status=get_text_by_id(tree, "status"),
                    judge=get_text_by_id(tree, "judge"),
                    court_reporter=get_text_by_id(tree, "court-reporter"),
                )
            )


What's Next
-----------

In :doc:`10_transient_exceptions`, we'll take a look at transient exceptions
like response timeouts.