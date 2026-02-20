Step 10: Transient Exceptions
===============================

In Step 9, we introduced data validation to ensure scraped data conforms to
expected schemas. Now we address another operational concern: **what happens
when temporary server errors occur?**

Web scraping encounters transient failures: server errors (5xx status codes),
network timeouts, and temporary service disruptions. These failures are
fundamentally different from structural assumptions (Step 8) or data validation
errors (Step 9) - they're temporary and might resolve on retry.

This step introduces **transient exceptions** to distinguish temporary failures
from permanent errors, enabling appropriate error handling and retry strategies.


Overview
--------

In this step, we introduce:

1. **TransientException** - Base exception class for temporary errors
2. **HTMLResponseAssumptionException** - Exception for unexpected HTTP status codes
3. **RequestTimeoutException** - Exception for request timeouts
4. **Driver-side detection** - SyncDriver raises transient exceptions for 5xx errors


Why Transient Exceptions?
--------------------------

Different types of errors require different handling strategies:

**Permanent Errors** (don't retry):

- HTMLStructuralAssumptionException - Website structure changed, scraper needs updating
- DataFormatAssumptionException - Data format changed, model needs updating
- 404 Not Found - Resource doesn't exist

**Transient Errors** (retry might succeed):

- 429 Too Many Requests - Rate limiting, retry after delay
- 500 Internal Server Error - Temporary server problem
- 503 Service Unavailable - Server overloaded or maintenance
- Network timeout - Slow network or server
- Connection reset - Temporary network issue

By distinguishing transient from permanent errors, we can:

- **Retry intelligently** - Only retry errors that might resolve
- **Monitor effectively** - Track transient vs permanent failure rates
- **Alert appropriately** - High transient error rates suggest infrastructure issues


Exception Hierarchy
-------------------

TransientException (Base)
^^^^^^^^^^^^^^^^^^^^^^^^^^

The base class for all transient errors:

.. code-block:: python

    class TransientException(Exception):
        """Base class for transient errors that might resolve on retry.

        Transient exceptions represent temporary failures like network issues,
        server errors (5xx), or timeouts. Unlike assumption exceptions which
        indicate scraper code needs updating, transient exceptions suggest
        retrying the request may succeed.

        The driver is responsible for retry logic and strategy.
        """
        pass

Features:

- **Simple inheritance** - Standard Exception subclass
- **Clear semantics** - Signals "this might work if we try again"
- **Driver responsibility** - Retry logic lives in driver, not scraper


HTMLResponseAssumptionException
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Exception for unexpected HTTP status codes:

.. code-block:: python

    class HTMLResponseAssumptionException(TransientException):
        """Raised when HTTP response has unexpected status code.

        This exception indicates the server returned a status code we didn't
        expect. Server errors (5xx) are transient, but client errors (4xx)
        might indicate a permanent problem.

        Attributes:
            status_code: The actual HTTP status code received.
            expected_codes: List of status codes that were expected.
            url: The URL that returned the unexpected status.
            message: Human-readable error message.
        """

        def __init__(
            self,
            status_code: int,
            expected_codes: list[int],
            url: str,
        ) -> None:
            self.status_code = status_code
            self.expected_codes = expected_codes
            self.url = url

            expected_str = ", ".join(str(code) for code in expected_codes)
            self.message = (
                f"HTTP {status_code} from {url} "
                f"(expected one of: {expected_str})"
            )
            super().__init__(self.message)


RequestTimeoutException
^^^^^^^^^^^^^^^^^^^^^^^

Exception for request timeouts:

.. code-block:: python

    class RequestTimeoutException(TransientException):
        """Raised when a request times out.

        This exception indicates the request took longer than the configured
        timeout. Network issues or slow servers can cause timeouts. Retrying
        may succeed.

        Attributes:
            url: The URL that timed out.
            timeout_seconds: The timeout duration in seconds.
            message: Human-readable error message.
        """

        def __init__(self, url: str, timeout_seconds: float) -> None:
            self.url = url
            self.timeout_seconds = timeout_seconds
            self.message = f"Request to {url} timed out after {timeout_seconds}s"
            super().__init__(self.message)

Example error message:

.. code-block:: text

    Request to http://example.com/slow-endpoint timed out after 30.0s


Driver Implementation
---------------------

SyncDriver Detection
^^^^^^^^^^^^^^^^^^^^

The SyncDriver now checks HTTP status codes and raises
``HTMLResponseAssumptionException`` for server errors:

.. code-block:: python

    def resolve_request(self, request: Request) -> Response:
        """Fetch a Request and return the Response.

        Args:
            request: The request to fetch.

        Returns:
            Response containing the HTTP response data.

        Raises:
            HTMLResponseAssumptionException: If server returns 5xx status code.
        """
        http_params = request.request
        with httpx.Client() as client:
            http_response = client.request(
                method=http_params.method.value,
                url=http_params.url,
                headers=http_params.headers if http_params.headers else None,
                content=http_params.data if isinstance(http_params.data, bytes) else None,
                data=http_params.data if isinstance(http_params.data, dict) else None,
            )

        # Step 10: Check for server errors (5xx status codes) and rate limiting (429)
        if http_response.status_code >= 500 or http_response.status_code == 429:
            raise HTMLResponseAssumptionException(
                status_code=http_response.status_code,
                expected_codes=[200],
                url=http_params.url,
            )

        return Response(
            status_code=http_response.status_code,
            headers=dict(http_response.headers),
            content=http_response.content,
            text=http_response.text,
            url=http_params.url,
            request=request,
        )

**Key behaviors:**

- **All 5xx codes** - Raises for any status >= 500
- **429 Too Many Requests** - Also raised for rate limiting (status code 429)
- **Expected codes** - Currently expects [200], but could be extended
- **Before Response creation** - Exception raised before returning Response


Timeout Handling
^^^^^^^^^^^^^^^^

The SyncDriver catches httpx timeout exceptions and converts them to
``RequestTimeoutException``:

.. code-block:: python

    try:
        with httpx.Client() as client:
            http_response = client.request(...)
    except httpx.TimeoutException as e:
        # Convert httpx timeout to our RequestTimeoutException
        timeout_seconds = 30.0  # Default timeout
        raise RequestTimeoutException(
            url=http_params.url,
            timeout_seconds=timeout_seconds,
        ) from e

This ensures all timeout errors are represented as ``TransientException``
subclasses, enabling consistent error handling.


Callback Integration
^^^^^^^^^^^^^^^^^^^^

The SyncDriver wraps request resolution in a try-except block to catch
transient exceptions and invoke the ``on_transient_exception`` callback:

.. code-block:: python

    while self.request_queue:
        request: Request = self.request_queue.pop(0)

        # Step 10: Wrap request resolution to catch transient exceptions
        try:
            response: Response = (
                self.resolve_archive_request(request)
                if request.archive
                else self.resolve_request(request)
            )
        except TransientException as e:
            # Step 10: Handle transient errors via callback
            if self.on_transient_exception:
                # Invoke callback - if it returns False, stop scraping
                should_continue = self.on_transient_exception(e)
                if not should_continue:
                    return
                # If callback returns True, continue processing next request
                continue
            else:
                # No callback provided - propagate exception normally
                raise

        # Continue with normal processing...

**Behavior:**

- **Callback returns True** - Skip failed request, continue with next request in queue
- **Callback returns False** - Stop scraping immediately
- **No callback** - Exception propagates and stops scraper
- **Transient exception** - Callback invoked before continuation method is called


**Callback behavior:**

- **Return True** - Continue scraping remaining requests
- **Return False** - Stop scraping immediately
- **Not provided** - Exceptions propagate normally (stop scraping)


Next Steps
----------

In :doc:`11_archive_callback`, we'll introduce archive callbacks for handling
file storage and archival during scraping.