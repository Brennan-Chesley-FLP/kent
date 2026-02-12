"""Exception types for scraper errors.

This module defines exception hierarchy for scraper assumption violations.
Step 8 introduces structural assumption errors for HTML parsing.
"""

from typing import Any


class ScraperAssumptionException(Exception):
    """Base class for scraper assumption violations.

    Scrapers make assumptions about website structure, data formats, and
    navigation patterns. When these assumptions are violated, they should
    raise clear, contextual exceptions that help diagnose the issue.

    This is the base class for all assumption violations. Subclasses should
    provide specific context about what assumption was violated.
    """

    def __init__(
        self,
        message: str,
        request_url: str,
        context: dict[str, Any] | None = None,
    ) -> None:
        """Initialize the exception.

        Args:
            message: Human-readable description of the assumption violation.
            request_url: The URL of the request that triggered this error.
            context: Optional dict of additional context (selector, counts, etc).
        """
        self.message = message
        self.request_url = request_url
        self.context = context or {}
        super().__init__(self._format_message())

    def _format_message(self) -> str:
        """Format the error message with context.

        Returns:
            Formatted error message string.
        """
        parts = [self.message]
        parts.append(f"URL: {self.request_url}")

        if self.context:
            parts.append("Context:")
            for key, value in self.context.items():
                parts.append(f"  {key}: {value}")

        return "\n".join(parts)


class HTMLStructuralAssumptionException(ScraperAssumptionException):
    """Raised when HTML structure doesn't match expectations.

    This exception is raised when XPath or CSS selectors return a different
    number of elements than expected. This usually indicates that the website's
    HTML structure has changed.

    Attributes:
        selector: The XPath or CSS selector that was used.
        selector_type: Type of selector ("xpath" or "css").
        is_element_query: True if querying for elements, False for strings/attributes.
    """

    def __init__(
        self,
        selector: str,
        selector_type: str,
        description: str,
        expected_min: int,
        expected_max: int | None,
        actual_count: int,
        request_url: str,
        is_element_query: bool = True,
    ) -> None:
        """Initialize the exception.

        Args:
            selector: The XPath or CSS selector that was used.
            selector_type: Type of selector ("xpath" or "css").
            description: Human-readable description of what was being selected.
            expected_min: Minimum number of elements expected.
            expected_max: Maximum number of elements expected (None = unlimited).
            actual_count: Actual number of elements found.
            request_url: The URL of the request that triggered this error.
            is_element_query: True if querying for elements (default), False for strings.
        """
        self.selector = selector
        self.selector_type = selector_type
        self.description = description
        self.expected_min = expected_min
        self.expected_max = expected_max
        self.actual_count = actual_count
        self.is_element_query = is_element_query

        # Build expected count string
        if expected_max is None:
            expected_str = f"at least {expected_min}"
        elif expected_min == expected_max:
            expected_str = f"exactly {expected_min}"
        else:
            expected_str = f"between {expected_min} and {expected_max}"

        message = (
            f"HTML structure mismatch: Expected {expected_str} "
            f"elements for '{description}', but found {actual_count}"
        )

        context = {
            "selector": selector,
            "selector_type": selector_type,
            "expected_min": expected_min,
            "expected_max": expected_max
            if expected_max is not None
            else "unlimited",
            "actual_count": actual_count,
            "is_element_query": is_element_query,
        }

        super().__init__(message, request_url, context)


class DataFormatAssumptionException(ScraperAssumptionException):
    """Raised when scraped data doesn't match expected schema.

    This exception is raised during Pydantic validation when the scraped
    data doesn't conform to the expected data model. This indicates that
    the website's data format has changed or the scraper's extraction
    logic needs updating.
    """

    def __init__(
        self,
        errors: list[dict[str, Any]],
        failed_doc: dict[str, Any],
        model_name: str,
        request_url: str,
    ) -> None:
        """Initialize the exception.

        Args:
            errors: List of Pydantic validation errors.
            failed_doc: The document that failed validation.
            model_name: Name of the Pydantic model that was being validated against.
            request_url: The URL of the request that produced this data.
        """
        self.errors = errors
        self.failed_doc = failed_doc
        self.model_name = model_name

        # Build human-readable error summary
        error_summary = ", ".join(
            f"{err['loc'][0]}: {err['msg']}" for err in errors
        )

        message = (
            f"Data validation failed for model '{model_name}': {error_summary}"
        )

        context = {
            "model": model_name,
            "error_count": len(errors),
            "errors": errors,
            "failed_doc": failed_doc,
        }

        super().__init__(message, request_url, context)


# =============================================================================
# Step 10: Transient Exceptions
# =============================================================================


class TransientException(Exception):
    """Base class for transient errors that might resolve on retry.

    Transient exceptions represent temporary failures like network issues,
    server errors (5xx), or timeouts. Unlike assumption exceptions which
    indicate scraper code needs updating, transient exceptions suggest
    retrying the request may succeed.

    The driver is responsible for retry logic and strategy.
    """

    pass


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
        """Initialize the exception.

        Args:
            status_code: The actual status code received.
            expected_codes: List of expected status codes.
            url: The URL of the request.
        """
        self.status_code = status_code
        self.expected_codes = expected_codes
        self.url = url

        expected_str = ", ".join(str(code) for code in expected_codes)
        self.message = (
            f"HTTP {status_code} from {url} (expected one of: {expected_str})"
        )
        super().__init__(self.message)


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
        """Initialize the exception.

        Args:
            url: The URL that timed out.
            timeout_seconds: The timeout duration in seconds.
        """
        self.url = url
        self.timeout_seconds = timeout_seconds
        self.message = f"Request to {url} timed out after {timeout_seconds}s"
        super().__init__(self.message)


class RequestFailedHalt(Exception):
    pass


class RequestFailedSkip(Exception):
    pass
