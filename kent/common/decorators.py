"""Step and entry decorators for scraper methods.

Step 19 introduces a flexible @step decorator that uses argument inspection
to determine what to inject into scraper methods. Instead of having separate
decorators for each content type (lxml, json, text, etc.), a single decorator
inspects the function signature and injects values based on parameter names.

Supported parameter names:

- response: The Response object
- request: The current BaseRequest
- previous_request: The parent request from the chain
- accumulated_data: Data collected across the request chain (from request)
- aux_data: Navigation metadata like tokens, session data (from request)
- json_content: Response content parsed as JSON
- lxml_tree: Response content parsed as CheckedHtmlElement
- text: Response content as string
- local_filepath: Local file path from ArchiveResponse (None if not archive)

The decorator also handles:

- Attaching priority metadata to functions
- Attaching encoding, xsd, and json_model metadata for drivers to optionally use
- Auto-resolving Callable continuations to string names
- Automatic yielding from wrapped generators

The @entry decorator marks scraper methods as entry points with typed
parameters, replacing the old get_entry()/ScraperParams system.
"""

import inspect
import json
from collections.abc import Callable, Generator
from dataclasses import dataclass
from datetime import date
from functools import wraps
from typing import Any, TypeVar, cast, get_type_hints

from lxml import html as lxml_html
from pydantic import BaseModel

from kent.common.checked_html import CheckedHtmlElement
from kent.common.exceptions import (
    ScraperAssumptionException,
)
from kent.common.speculation_types import (
    SimpleSpeculation,
    SpeculationType,
    YearlySpeculation,
)
from kent.data_types import (
    ArchiveResponse,
    BaseRequest,
    Request,
    Response,
    ScraperYield,
)

T = TypeVar("T")


class StepMetadata:
    """Metadata attached to scraper step methods by @step decorator.

    Attributes:
        priority: Priority hint for queue ordering (lower = higher priority).
        encoding: Character encoding for text/HTML decoding.
        xsd: Optional path to XSD schema file for structural validation hints.
        json_model: Optional dotted path to Pydantic model for JSON response validation.
        await_list: List of wait conditions for Playwright driver (WaitForSelector, etc).
        auto_await_timeout: Optional timeout in milliseconds for autowait retry logic.
        observer: Optional SelectorObserver for debugging and autowait (set after step execution).
    """

    def __init__(
        self,
        priority: int = 9,
        encoding: str = "utf-8",
        xsd: str | None = None,
        json_model: str | None = None,
        await_list: list[Any] | None = None,
        auto_await_timeout: int | None = None,
    ):
        self.priority = priority
        self.encoding = encoding
        self.xsd = xsd
        self.json_model = json_model
        self.await_list = await_list or []
        self.auto_await_timeout = auto_await_timeout
        self.observer: Any = None  # Will be set after step execution


class SpeculateMetadata:
    """Metadata attached to scraper methods by @speculate decorator.

    Attributes:
        observation_date: Date when metadata was last updated (e.g., when gap was observed).
        highest_observed: The highest ID observed to exist.
        largest_observed_gap: The largest gap observed in the sequence.
    """

    def __init__(
        self,
        observation_date: date | None = None,
        highest_observed: int = 1,
        largest_observed_gap: int = 10,
    ):
        self.observation_date = observation_date
        self.highest_observed = highest_observed
        self.largest_observed_gap = largest_observed_gap


def _parse_json(response: Response) -> Any:
    """Parse JSON from response content.

    Args:
        response: The HTTP response.

    Returns:
        Parsed JSON data (dict, list, or other JSON types).

    Raises:
        ScraperAssumptionException: If JSON parsing fails.
    """
    try:
        text = response.text or response.content.decode("utf-8")
        return json.loads(text)
    except Exception as e:
        raise ScraperAssumptionException(
            f"Failed to parse JSON: {e}",
            request_url=response.url,
            context={"error": str(e)},
        ) from e


def _parse_html(
    response: Response, encoding: str = "utf-8"
) -> CheckedHtmlElement:
    """Parse HTML from response content.

    Passes raw bytes to lxml so it can auto-detect encoding from the HTML
    meta charset tag (e.g., <meta charset="windows-1252">). This handles
    pages that declare non-UTF-8 encodings correctly.

    Args:
        response: The HTTP response.
        encoding: Fallback encoding if lxml can't detect one (default utf-8).

    Returns:
        CheckedHtmlElement parsed from response content.

    Raises:
        ScraperAssumptionException: If HTML parsing fails.
    """
    try:
        # Pass raw bytes to lxml - it will detect encoding from:
        # 1. BOM
        # 2. XML declaration
        # 3. <meta charset="..."> or <meta http-equiv="Content-Type" content="...">
        # 4. Falls back to default if nothing found
        return CheckedHtmlElement(
            lxml_html.fromstring(response.content), response.url
        )
    except Exception as e:
        raise ScraperAssumptionException(
            f"Failed to parse HTML: {e}",
            request_url=response.url,
            context={"encoding": encoding, "error": str(e)},
        ) from e


def _get_text(response: Response, encoding: str = "utf-8") -> str:
    """Get text content from response.

    Args:
        response: The HTTP response.
        encoding: Character encoding for decoding.

    Returns:
        Response text as string.
    """
    if response.text is not None:
        return response.text
    return response.content.decode(encoding)


def _parse_page_element(
    response: Response, encoding: str = "utf-8"
) -> tuple[Any, Any]:
    """Parse HTML and create PageElement with SelectorObserver.

    Args:
        response: The HTTP response.
        encoding: Fallback encoding if lxml can't detect one.

    Returns:
        Tuple of (PageElement, SelectorObserver) for injection and debugging.

    Raises:
        ScraperAssumptionException: If HTML parsing fails.
    """
    from kent.common.lxml_page_element import (
        LxmlPageElement,
    )
    from kent.common.selector_observer import (
        SelectorObserver,
    )

    try:
        # Parse HTML using lxml and wrap in CheckedHtmlElement
        checked_element = _parse_html(response, encoding)

        # Create observer to track selector queries
        observer = SelectorObserver()

        # Create PageElement with observer
        page_element = LxmlPageElement(
            element=checked_element, url=response.url, observer=observer
        )

        return page_element, observer
    except Exception as e:
        raise ScraperAssumptionException(
            f"Failed to parse HTML for page element: {e}",
            request_url=response.url,
            context={"encoding": encoding, "error": str(e)},
        ) from e


def _process_yielded_request(yielded: Any) -> Any:
    """Process a yielded BaseRequest to resolve Callable continuations.

    When a decorated function yields a BaseRequest with a Callable continuation,
    this resolves it to the function name and attaches the target step's priority.

    Args:
        yielded: The value yielded by the step.

    Returns:
        The processed yield value.
    """
    if (
        isinstance(yielded, BaseRequest)
        and callable(yielded.continuation)
        and not isinstance(yielded.continuation, str)
    ):
        # Get the target function's step metadata (if decorated with @step)
        target_metadata = get_step_metadata(yielded.continuation)

        # Resolve Callable to function name
        func_name = yielded.continuation.__name__
        # Note: We use object.__setattr__ because dataclasses are frozen
        object.__setattr__(yielded, "continuation", func_name)

        # If the yielded request doesn't have a priority set,
        # inherit from the target step's metadata
        if yielded.priority == 9 and target_metadata is not None:
            object.__setattr__(yielded, "priority", target_metadata.priority)

    return yielded


def step(
    func: Callable[..., Generator[ScraperYield, Any, None]] | None = None,
    *,
    priority: int = 9,
    encoding: str = "utf-8",
    xsd: str | None = None,
    json_model: str | None = None,
    await_list: list[Any] | None = None,
    auto_await_timeout: int | None = None,
) -> Any:
    """Decorator for scraper step methods with automatic argument injection.

    This decorator inspects the function signature and injects values based on
    parameter names:

    - response: The Response object
    - request: The current BaseRequest
    - previous_request: The parent request from the chain (if available)
    - accumulated_data: Data collected across the request chain (from request)
    - aux_data: Navigation metadata like tokens, session data (from request)
    - json_content: Response content parsed as JSON
    - lxml_tree: Response content parsed as CheckedHtmlElement
    - page: Response content parsed as PageElement (LxmlPageElement with observer)
    - text: Response content as string
    - local_filepath: Local file path from ArchiveResponse (None otherwise)

    Example::

        @step
        def parse_page(self, lxml_tree: CheckedHtmlElement, response: Response):
            # lxml_tree and response are automatically injected
            cases = lxml_tree.checked_xpath("//div[@class='case']", "cases")
            for case in cases:
                yield ParsedData(...)

        @step(priority=5)
        def parse_api(self, json_content: dict, response: Response):
            # json_content and response are automatically injected
            for item in json_content['items']:
                yield ParsedData(...)

        @step
        def parse_with_callable(self, text: str):
            # Can yield requests with Callable continuations
            yield Request(
                url="/next",
                continuation=self.parse_next_page  # Callable!
            )

        @step(xsd="schemas/court_page.xsd")
        def parse_court_page(self, lxml_tree: CheckedHtmlElement):
            # XSD reference available via get_step_metadata() for drivers
            # to optionally use when evaluating structural errors
            ...

        @step(json_model="api.publications.PublicationsResponse")
        def parse_api_response(self, json_content: dict):
            # JSON model reference available via get_step_metadata() for drivers
            # to optionally use for post-hoc validation
            ...

    Args:
        func: The scraper step method to decorate (when used without parens).
        priority: Priority hint for queue ordering (lower = higher priority).
        encoding: Character encoding for text/HTML decoding.
        xsd: Optional path to XSD schema file. Drivers may use this hint
            when evaluating structural assumption errors.
        json_model: Optional dotted path to Pydantic model (e.g.,
            "api.publications.PublicationsResponse"). Resolved relative to
            scraper package. Drivers may use this for post-hoc validation.
        await_list: Optional list of wait conditions for Playwright driver
            (WaitForSelector, WaitForLoadState, WaitForURL, WaitForTimeout).
            HTTP driver ignores this parameter.
        auto_await_timeout: Optional timeout in milliseconds for autowait retry logic.
            When set, Playwright driver will retry the step if it raises
            HTMLStructuralAssumptionException. HTTP driver ignores this parameter.

    Returns:
        Decorated function with automatic argument injection.

    Raises:
        ScraperAssumptionException: If content parsing fails.
    """

    def decorator(
        fn: Callable[..., Generator[ScraperYield, Any, None]],
    ) -> Callable[..., Generator[ScraperYield, bool | None, None]]:
        # Inspect the function signature to determine what to inject
        sig = inspect.signature(fn)
        param_names = [p.name for p in sig.parameters.values()]

        # Create metadata
        metadata = StepMetadata(
            priority=priority,
            encoding=encoding,
            xsd=xsd,
            json_model=json_model,
            await_list=await_list,
            auto_await_timeout=auto_await_timeout,
        )

        @wraps(fn)
        def wrapper(
            scraper_self: Any,
            response: Response,
            *args: Any,
            **kwargs: Any,
        ) -> Generator[ScraperYield, bool | None, None]:
            # Build kwargs for injection based on parameter names
            injected_kwargs: dict[str, Any] = {}
            observer = None  # Track observer for metadata storage

            if "response" in param_names:
                injected_kwargs["response"] = response

            if "request" in param_names:
                injected_kwargs["request"] = response.request

            if "previous_request" in param_names:
                # Get the previous request from the chain
                if response.request.previous_requests:
                    injected_kwargs["previous_request"] = (
                        response.request.previous_requests[-1]
                    )
                else:
                    injected_kwargs["previous_request"] = None

            if "accumulated_data" in param_names:
                injected_kwargs["accumulated_data"] = (
                    response.request.accumulated_data
                )

            if "aux_data" in param_names:
                injected_kwargs["aux_data"] = response.request.aux_data

            # Content transformations (lazy - only parse if requested)
            if "json_content" in param_names:
                injected_kwargs["json_content"] = _parse_json(response)

            if "lxml_tree" in param_names:
                injected_kwargs["lxml_tree"] = _parse_html(response, encoding)

            if "page" in param_names:
                page_element, observer = _parse_page_element(
                    response, encoding
                )
                injected_kwargs["page"] = page_element

            if "text" in param_names:
                injected_kwargs["text"] = _get_text(response, encoding)

            if "local_filepath" in param_names:
                if isinstance(response, ArchiveResponse):
                    injected_kwargs["local_filepath"] = response.file_url
                else:
                    injected_kwargs["local_filepath"] = None

            # Call the original function with injected kwargs
            gen = fn(scraper_self, *args, **injected_kwargs, **kwargs)

            # Yield from the generator, processing requests to resolve Callables
            try:
                for yielded in gen:
                    processed = _process_yielded_request(yielded)
                    yield processed
            finally:
                # Store observer in metadata for driver access (debugging/autowait)
                if observer is not None:
                    metadata.observer = observer

        # Attach metadata to the wrapper
        wrapper._step_metadata = metadata  # type: ignore[attr-defined]
        return wrapper

    # Support both @step and @step(priority=5) syntax
    if func is not None:
        return decorator(func)
    return decorator


def get_step_metadata(func: Callable[..., Any]) -> StepMetadata | None:
    """Get step metadata from a decorated method.

    Args:
        func: A potentially decorated scraper step method.

    Returns:
        StepMetadata if the method is decorated, None otherwise.
    """
    return getattr(func, "_step_metadata", None)


def is_step(func: Callable[..., Any]) -> bool:
    """Check if a method is a decorated step.

    Args:
        func: A method to check.

    Returns:
        True if the method has step decorator metadata.
    """
    return get_step_metadata(func) is not None


def speculate(
    func: Callable[[Any, int], BaseRequest] | None = None,
    *,
    observation_date: date | None = None,
    highest_observed: int = 1,
    largest_observed_gap: int = 10,
) -> Any:
    """Decorator for functions that generate speculative requests from sequential IDs.

    The @speculate decorator marks functions that generate speculative requests.
    These functions accept a single integer parameter (the ID) and return a
    Request with is_speculative=True.

    Drivers use these functions to seed their initial queues and track which
    IDs have been successfully fetched.

    Example::

        @speculate(highest_observed=500, largest_observed_gap=20)
        def fetch_case(self, case_id: int) -> Request:
            return Request(
                url=f"/case/{case_id}",
                continuation=self.parse_case
            )

    Args:
        func: The function to decorate (when used without parens).
        observation_date: Date when metadata was last updated.
        highest_observed: The highest ID observed to exist (defaults to 1).
        largest_observed_gap: The largest gap observed in the sequence (defaults to 10).

    Returns:
        Decorated function with SpeculateMetadata attached.
    """

    def decorator(
        fn: Callable[[Any, int], BaseRequest],
    ) -> Callable[[Any, int], BaseRequest]:
        # Inspect the function signature
        sig = inspect.signature(fn)
        params = list(sig.parameters.values())

        # Validate: function should have exactly one parameter (besides self)
        # The first parameter should be 'self' for instance methods
        if len(params) < 1:
            raise TypeError(
                f"Speculate function '{fn.__name__}' must accept at least one parameter "
                f"(the ID). Signature: {sig}"
            )

        # For instance methods, we expect (self, id)
        # For standalone functions, we expect (id,)
        if len(params) == 1:
            # Standalone function - single ID parameter
            pass
        elif len(params) == 2:
            # Instance method - self + ID parameter
            pass
        else:
            raise TypeError(
                f"Speculate function '{fn.__name__}' must accept exactly one parameter "
                f"(the ID) in addition to self. Got {len(params) - 1} parameters. "
                f"Signature: {sig}"
            )

        # Create metadata
        metadata = SpeculateMetadata(
            observation_date=observation_date,
            highest_observed=highest_observed,
            largest_observed_gap=largest_observed_gap,
        )

        @wraps(fn)
        def wrapper(scraper_self: Any, id_value: int) -> Request:
            # Call the original function
            request = fn(scraper_self, id_value)

            # Ensure the returned value is a Request
            if not isinstance(request, Request):
                raise TypeError(
                    f"Speculate function '{fn.__name__}' must return a Request, "
                    f"got {type(request).__name__}"
                )

            # Use the speculative() method to create a copy with speculation fields set
            # This sets is_speculative=True and speculation_id=(func_name, id_value)
            return request.speculative(fn.__name__, id_value)

        # Attach metadata to the wrapper
        wrapper._speculate_metadata = metadata  # type: ignore[attr-defined]
        return wrapper

    # Support both @speculate and @speculate(...) syntax
    if func is not None:
        return decorator(func)
    return decorator


def get_speculate_metadata(
    func: Callable[..., Any],
) -> SpeculateMetadata | None:
    """Get speculate metadata from a decorated function.

    Args:
        func: A potentially decorated speculate function.

    Returns:
        SpeculateMetadata if the function is decorated, None otherwise.
    """
    return getattr(func, "_speculate_metadata", None)


def is_speculate(func: Callable[..., Any]) -> bool:
    """Check if a method is a decorated speculate function.

    Args:
        func: A method to check.

    Returns:
        True if the method has speculate decorator metadata.
    """
    return get_speculate_metadata(func) is not None


# =============================================================================
# @entry decorator for scraper entry points
# =============================================================================

# Allowed primitive types for @entry parameters
_ENTRY_PRIMITIVE_TYPES = (str, int, date)


@dataclass(frozen=True)
class EntryMetadata:
    """Metadata attached to scraper entry point methods by @entry decorator.

    Attributes:
        return_type: The data type this entry produces (e.g. Docket).
        param_types: Mapping of parameter name to type (BaseModel subclass or primitive).
        func_name: Name of the decorated function.
        speculation: Speculation config (SimpleSpeculation, YearlySpeculation, or None).
    """

    return_type: type
    param_types: dict[str, type]
    func_name: str
    speculation: SpeculationType = None

    @property
    def speculative(self) -> bool:
        """Whether this is a speculative entry point."""
        return self.speculation is not None

    def validate_params(self, kwargs_dict: dict[str, Any]) -> dict[str, Any]:
        """Validate and coerce parameters for this entry function.

        For non-speculative entries, validates against the function signature:
        BaseModel parameters use model_validate(), primitives are coerced.

        For speculative entries, validates against the range schema:
        - SimpleSpeculation: single param mapped to [start, end] range
        - YearlySpeculation: year (int) + speculative param as [start, end] + optional frozen (bool)

        Args:
            kwargs_dict: Raw parameter dict from JSON deserialization.

        Returns:
            Dict of validated/coerced parameter values ready for function call
            (non-speculative) or range config dict (speculative).

        Raises:
            pydantic.ValidationError: If a BaseModel parameter fails validation.
            TypeError: If a primitive parameter can't be coerced.
            ValueError: If unexpected parameters are provided.
        """
        if self.speculation is not None:
            return self._validate_speculative_params(kwargs_dict)
        return self._validate_direct_params(kwargs_dict)

    def _validate_direct_params(
        self, kwargs_dict: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate params for direct function invocation."""
        validated: dict[str, Any] = {}

        # Check for unexpected parameters
        unexpected = set(kwargs_dict.keys()) - set(self.param_types.keys())
        if unexpected:
            raise ValueError(
                f"Unexpected parameters for entry '{self.func_name}': "
                f"{unexpected}. Expected: {list(self.param_types.keys())}"
            )

        for param_name, param_type in self.param_types.items():
            if param_name not in kwargs_dict:
                raise ValueError(
                    f"Missing required parameter '{param_name}' "
                    f"for entry '{self.func_name}'"
                )

            raw_value = kwargs_dict[param_name]

            if isinstance(param_type, type) and issubclass(
                param_type, BaseModel
            ):
                # Pydantic model: validate via model_validate
                pydantic_type = cast(type[BaseModel], param_type)
                validated[param_name] = pydantic_type.model_validate(raw_value)
            elif param_type is date:
                # date: accept date objects or ISO format strings
                if isinstance(raw_value, date):
                    validated[param_name] = raw_value
                elif isinstance(raw_value, str):
                    validated[param_name] = date.fromisoformat(raw_value)
                else:
                    raise TypeError(
                        f"Parameter '{param_name}' for entry "
                        f"'{self.func_name}' expected date or ISO string, "
                        f"got {type(raw_value).__name__}"
                    )
            elif param_type in (str, int):
                # Primitive: coerce
                validated[param_name] = param_type(raw_value)
            else:
                # Shouldn't happen if decorator validation is correct
                validated[param_name] = raw_value

        return validated

    def _validate_speculative_params(
        self, kwargs_dict: dict[str, Any]
    ) -> dict[str, Any]:
        """Validate params as range config for speculative entries.

        For SimpleSpeculation: expect {param_name: [start, end]}
        For YearlySpeculation: expect {year: int, param_name: [start, end], frozen?: bool}
        """
        validated: dict[str, Any] = {}

        if isinstance(self.speculation, SimpleSpeculation):
            # Single param, must be a 2-element range
            (param_name,) = self.param_types.keys()
            expected_keys = {param_name}
            unexpected = set(kwargs_dict.keys()) - expected_keys
            if unexpected:
                raise ValueError(
                    f"Unexpected parameters for speculative entry "
                    f"'{self.func_name}': {unexpected}. "
                    f"Expected: {list(expected_keys)}"
                )
            if param_name not in kwargs_dict:
                raise ValueError(
                    f"Missing required parameter '{param_name}' "
                    f"for speculative entry '{self.func_name}'"
                )
            raw_range = kwargs_dict[param_name]
            validated[param_name] = _validate_int_range(
                raw_range, param_name, self.func_name
            )

        elif isinstance(self.speculation, YearlySpeculation):
            # year param + speculative axis param + optional frozen
            spec_axis = _get_speculative_axis(self.param_types)
            expected_keys = {"year", spec_axis, "frozen"}
            unexpected = set(kwargs_dict.keys()) - expected_keys
            if unexpected:
                raise ValueError(
                    f"Unexpected parameters for speculative entry "
                    f"'{self.func_name}': {unexpected}. "
                    f"Expected: year, {spec_axis}, and optionally frozen"
                )

            if "year" not in kwargs_dict:
                raise ValueError(
                    f"Missing required parameter 'year' "
                    f"for speculative entry '{self.func_name}'"
                )
            validated["year"] = int(kwargs_dict["year"])

            if spec_axis not in kwargs_dict:
                raise ValueError(
                    f"Missing required parameter '{spec_axis}' "
                    f"for speculative entry '{self.func_name}'"
                )
            validated[spec_axis] = _validate_int_range(
                kwargs_dict[spec_axis], spec_axis, self.func_name
            )

            if "frozen" in kwargs_dict:
                validated["frozen"] = bool(kwargs_dict["frozen"])
            else:
                validated["frozen"] = False

        return validated


def _validate_int_range(
    raw_value: Any, param_name: str, func_name: str
) -> tuple[int, int]:
    """Validate that a value is a 2-element integer range."""
    if not isinstance(raw_value, list | tuple) or len(raw_value) != 2:
        raise ValueError(
            f"Parameter '{param_name}' for speculative entry "
            f"'{func_name}' must be a 2-element [start, end] range, "
            f"got {raw_value!r}"
        )
    return (int(raw_value[0]), int(raw_value[1]))


def _get_speculative_axis(param_types: dict[str, type]) -> str:
    """Get the speculative axis param name (the one that isn't 'year')."""
    for name in param_types:
        if name != "year":
            return name
    raise ValueError("No non-year parameter found in param_types")


def entry(
    return_type: type | Any,
    *,
    speculative: SpeculationType = None,
) -> Callable[..., Any]:
    """Decorator for scraper entry point methods with typed parameters.

    Marks a method as an entry point and attaches EntryMetadata describing
    the return type and parameter schema. Does NOT modify the function's
    runtime behavior.

    Parameters can be Pydantic BaseModel subclasses or primitives
    (str, int, date). Tuples are not supported.

    Example::

        @entry(Docket)
        def search_by_number(self, docket_number: str) -> Generator[Request, None, None]:
            ...

        @entry(Docket, speculative=SimpleSpeculation(highest_observed=105336, largest_observed_gap=20))
        def fetch_docket(self, crn: int) -> Request:
            ...

        @entry(Docket, speculative=YearlySpeculation(
            backfill=(YearPartition(year=2025, number=(1, 4000), frozen=False),),
            trailing_period=timedelta(days=60),
            largest_observed_gap=15,
        ))
        def docket_stabber(self, year: int, number: int) -> Request:
            ...

    Args:
        return_type: The data type this entry produces.
        speculative: Speculation config (SimpleSpeculation, YearlySpeculation, or None).

    Returns:
        Decorator that attaches EntryMetadata to the function.
    """

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        # Inspect function signature to extract parameter types
        # Skip 'self' for instance methods
        # Use get_type_hints with the function's module globals for proper
        # resolution when `from __future__ import annotations` is used
        hints: dict[str, Any] = {}
        try:
            module = inspect.getmodule(fn)
            globalns = getattr(module, "__dict__", None) if module else None
            hints = get_type_hints(fn, globalns=globalns)
        except Exception:
            # Fallback: try raw annotations (may be strings with PEP 563)
            try:
                hints = get_type_hints(fn)
            except Exception:
                hints = {}

        sig = inspect.signature(fn)
        param_types: dict[str, type] = {}

        for param_name, param in sig.parameters.items():
            if param_name == "self":
                continue
            if param_name == "return":
                continue

            # Get the type from hints, fallback to annotation
            param_type = hints.get(param_name)
            if param_type is None:
                # Try raw annotation (might be a string)
                ann = param.annotation
                if ann is inspect.Parameter.empty:
                    raise TypeError(
                        f"Entry function '{fn.__name__}' parameter "
                        f"'{param_name}' must have a type annotation"
                    )
                # If annotation is a string, try to resolve it
                if isinstance(ann, str):
                    module = inspect.getmodule(fn)
                    globalns = (
                        getattr(module, "__dict__", {}) if module else {}
                    )
                    try:
                        param_type = eval(ann, globalns)  # noqa: S307
                    except Exception:
                        raise TypeError(
                            f"Entry function '{fn.__name__}' parameter "
                            f"'{param_name}' has unresolvable type "
                            f"annotation '{ann}'"
                        ) from None
                else:
                    param_type = ann

            # Validate the parameter type
            if isinstance(param_type, type) and issubclass(
                param_type, BaseModel
            ):
                pass  # BaseModel subclass is fine
            elif param_type in _ENTRY_PRIMITIVE_TYPES:
                pass  # Primitive is fine
            elif param_type is tuple or (
                hasattr(param_type, "__origin__")
                and getattr(param_type, "__origin__", None) is tuple
            ):
                raise TypeError(
                    f"Entry function '{fn.__name__}' parameter "
                    f"'{param_name}' uses tuple type, which is not supported. "
                    f"Use a Pydantic BaseModel instead."
                )
            else:
                raise TypeError(
                    f"Entry function '{fn.__name__}' parameter "
                    f"'{param_name}' has unsupported type {param_type}. "
                    f"Use a Pydantic BaseModel subclass or one of: "
                    f"str, int, date"
                )

            param_types[param_name] = param_type

        # Validate speculation config against param_types
        if isinstance(speculative, SimpleSpeculation):
            non_self_params = list(param_types.items())
            if len(non_self_params) != 1 or non_self_params[0][1] is not int:
                raise TypeError(
                    f"Entry function '{fn.__name__}' with SimpleSpeculation "
                    f"must have exactly one int parameter (besides self). "
                    f"Got: {param_types}"
                )
        elif isinstance(speculative, YearlySpeculation):
            non_self_params = list(param_types.items())
            if len(non_self_params) != 2:
                raise TypeError(
                    f"Entry function '{fn.__name__}' with YearlySpeculation "
                    f"must have exactly two parameters (besides self). "
                    f"Got: {param_types}"
                )
            if "year" not in param_types:
                raise TypeError(
                    f"Entry function '{fn.__name__}' with YearlySpeculation "
                    f"must have a parameter named 'year'. "
                    f"Got: {list(param_types.keys())}"
                )
            for name, ptype in non_self_params:
                if ptype is not int:
                    raise TypeError(
                        f"Entry function '{fn.__name__}' with "
                        f"YearlySpeculation: parameter '{name}' must be int, "
                        f"got {ptype}"
                    )

        metadata = EntryMetadata(
            return_type=return_type,
            param_types=param_types,
            func_name=fn.__name__,
            speculation=speculative,
        )

        fn._entry_metadata = metadata  # type: ignore[attr-defined]
        return fn

    return decorator


def get_entry_metadata(func: Callable[..., Any]) -> EntryMetadata | None:
    """Get entry metadata from a decorated method.

    Args:
        func: A potentially decorated scraper entry method.

    Returns:
        EntryMetadata if the method is decorated with @entry, None otherwise.
    """
    return getattr(func, "_entry_metadata", None)


def is_entry(func: Callable[..., Any]) -> bool:
    """Check if a method is a decorated entry point.

    Args:
        func: A method to check.

    Returns:
        True if the method has entry decorator metadata.
    """
    return get_entry_metadata(func) is not None
