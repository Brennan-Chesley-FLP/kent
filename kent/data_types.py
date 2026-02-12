"""Data types for the scraper-driver architecture.

This module defines the core data types used for communication between
scrapers and drivers. These types are designed to be:

1. Exhaustive - Using Python 3.10's match statement to ensure all cases are handled
2. Serializable - Continuations are strings, not function references
3. Immutable - Dataclasses with frozen=True where appropriate

Step 1 introduces ParsedData.
Step 2 adds NavigatingRequest and Response.
Step 3 introduces BaseRequest, NonNavigatingRequest, and current_location tracking.
Step 4 adds ArchiveRequest and ArchiveResponse for file downloads.
Step 5 adds accumulated_data to BaseRequest with deep copy semantics.
Step 6 adds aux_data to BaseRequest for navigation metadata (tokens, session data).
"""

from __future__ import annotations

import hashlib
import json
import ssl
from collections.abc import Callable, Generator
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from http.cookiejar import CookieJar
from typing import (
    TYPE_CHECKING,
    Any,
    BinaryIO,
    ClassVar,
    Generic,
    TypeVar,
    cast,
)
from urllib.parse import quote, unquote, urljoin, urlparse, urlunparse

if TYPE_CHECKING:
    pass

# =============================================================================
# Step 1: ParsedData
# =============================================================================

T = TypeVar("T")
ScraperReturnType = TypeVar("ScraperReturnType")
ScraperParamType = TypeVar("ScraperParamType")


class ScraperStatus(Enum):
    """Status of a scraper's development lifecycle.

    Used for documentation and registry filtering.

    Values:
        IN_DEVELOPMENT: Scraper is being built, not ready for production.
        ACTIVE: Scraper is working and maintained.
        RETIRED: Scraper is no longer maintained (court changed, etc.).
    """

    IN_DEVELOPMENT = "in_development"
    ACTIVE = "active"
    RETIRED = "retired"


@dataclass(frozen=True)
class StepInfo:
    """Metadata about a scraper step method.

    Used by LocalDevDriver web interface to display available steps,
    their priorities, and to populate controls for pause_step/resume_step.

    Attributes:
        name: The method name (continuation string).
        priority: Priority hint for queue ordering (lower = higher priority).
        encoding: Character encoding for text/HTML decoding.
    """

    name: str
    priority: int
    encoding: str


@dataclass(frozen=True)
class EntryInfo:
    """Metadata about a scraper entry point method.

    Used by list_entries() to expose entry point metadata including
    return type, parameter types, and speculative flags.

    Attributes:
        name: The method name.
        return_type: The data type this entry produces.
        param_types: Mapping of parameter name to type.
        speculative: Whether this is a speculative entry.
        highest_observed: For speculative entries: highest known ID.
        largest_observed_gap: For speculative entries: largest gap.
    """

    name: str
    return_type: type
    param_types: dict[str, type]
    speculative: bool = False
    highest_observed: int = 1
    largest_observed_gap: int = 10


class BaseScraper(Generic[ScraperReturnType]):
    """Base class for all scrapers.

    Scrapers are generic over their return type, allowing drivers to
    be type-safe about what data they collect.

    Example:
        class MyScraper(BaseScraper[MyDataModel]):
            def parse_page(self, response: Response) -> Generator[ScraperYield, None, None]:
                yield ParsedData(MyDataModel(...))

    Class Attributes:
        court_ids: Set of court IDs this scraper covers (references courts.toml).
        court_url: The primary URL/origin for this scraper's court system.
        data_types: Set of data types this scraper produces (opinions, dockets, etc.).
        status: Development lifecycle status (IN_DEVELOPMENT, ACTIVE, RETIRED).
        version: Version string for this scraper (e.g., "2025-01-03").
        last_verified: Date when scraper was last verified working.
        oldest_record: Earliest date for which records are available.
        requires_auth: Whether authentication is required.
        msec_per_request_rate_limit: Minimum milliseconds between requests.
    """

    # === METADATA FOR AUTODOC ===
    # These ClassVars are used by the registry builder to generate documentation.

    court_ids: ClassVar[set[str]] = set()

    # Primary URL/origin for this scraper
    court_url: ClassVar[str] = ""

    # Data types produced by this scraper (e.g., {"opinions", "dockets"})
    data_types: ClassVar[set[str]] = set()

    # Scraper lifecycle status
    status: ClassVar[ScraperStatus] = ScraperStatus.IN_DEVELOPMENT

    # Version tracking
    version: ClassVar[str] = ""
    last_verified: ClassVar[str] = ""

    # Data availability
    oldest_record: ClassVar[date | None] = None

    # Optional metadata
    requires_auth: ClassVar[bool] = False
    msec_per_request_rate_limit: ClassVar[int | None] = None

    # SSL/TLS configuration for servers requiring specific ciphers or TLS versions.
    # If set, drivers will use this context for HTTPS connections.
    # Example usage for a scraper requiring specific ciphers:
    #     @classmethod
    #     def get_ssl_context(cls) -> ssl.SSLContext:
    #         ctx = ssl.create_default_context()
    #         ctx.set_ciphers("AES256-SHA256")
    #         return ctx
    ssl_context: ClassVar[ssl.SSLContext | None] = None

    def __init__(self, params: Any | None = None) -> None:
        """Initialize the scraper with optional search parameters.

        Args:
            params: Parameters for the scraper. During migration to @entry,
                this accepts the legacy ScraperParams or None.
        """
        self._params = params

    def get_params(self) -> Any | None:
        """Return the params instance for this scraper."""
        return self._params

    def get_entry(self) -> Generator[NavigatingRequest, None, None]:
        """Create the initial request(s) to start scraping.

        Subclasses should override this method (or use @entry decorators)
        to yield their entry point(s) and initial continuation method(s).

        Yields:
            NavigatingRequest for each entry point.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement get_entry() "
            f"or use @entry decorators"
        )

    @classmethod
    def params(cls) -> Any:
        """Build a params object for configuring scraper filters.

        Returns a stub params object for backward compat during
        migration to @entry decorators.
        """

        class _StubSpeculative:
            def __setattr__(self, name: str, value: Any) -> None:
                object.__setattr__(self, name, value)

        class _StubParams:
            def __init__(self) -> None:
                self.speculative = _StubSpeculative()

            def get_enabled_models(self) -> list[str]:
                return []

        return _StubParams()

    @classmethod
    def get_ssl_context(cls) -> ssl.SSLContext | None:
        """Return an SSL context for HTTPS connections, if needed.

        Override this method in scrapers that require custom SSL configuration
        (e.g., specific ciphers or TLS versions for legacy servers).

        Returns:
            An ssl.SSLContext configured for this scraper, or None to use defaults.

        Example::

            @classmethod
            def get_ssl_context(cls) -> ssl.SSLContext:
                ctx = ssl.create_default_context()
                ctx.set_ciphers("AES256-SHA256")
                return ctx
        """
        return cls.ssl_context

    def get_continuation(
        self, name: str
    ) -> Callable[
        [Response],
        Generator[ScraperYield[ScraperReturnType], bool | None, None],
    ]:
        """Resolve a continuation name to the actual method.

        This method looks up a continuation by name and returns the
        bound method. It provides a single point for continuation
        resolution, making it easy to add validation or caching later.

        Args:
            name: The name of the continuation method.

        Returns:
            The bound method that can be called with a Response.

        Raises:
            AttributeError: If the continuation method doesn't exist.
        """
        method = getattr(self, name)
        return cast(
            Callable[
                [Response],
                Generator[ScraperYield[ScraperReturnType], bool | None, None],
            ],
            method,
        )

    @classmethod
    def list_steps(cls) -> list[StepInfo]:
        """List all step methods defined on this scraper.

        Introspects the class to find all methods decorated with @step
        and returns their metadata.

        This is useful for the web interface to display available steps,
        their priorities, and to populate dropdowns for pause_step/resume_step.

        Returns:
            List of StepInfo objects for each decorated step method.

        Example:
            >>> class MyScraper(BaseScraper[CaseData]):
            ...     @step
            ...     def parse_listing(self, lxml_tree): ...
            ...
            ...     @step(priority=5)
            ...     def parse_detail(self, lxml_tree): ...
            ...
            >>> MyScraper.list_steps()
            [StepInfo(name='parse_listing', priority=9, encoding='utf-8'),
             StepInfo(name='parse_detail', priority=5, encoding='utf-8')]
        """
        from kent.common.decorators import (
            get_step_metadata,
        )

        steps = []
        for name in dir(cls):
            if name.startswith("_"):
                continue
            try:
                method = getattr(cls, name)
                metadata = get_step_metadata(method)
                if metadata is not None:
                    steps.append(
                        StepInfo(
                            name=name,
                            priority=metadata.priority,
                            encoding=metadata.encoding,
                        )
                    )
            except Exception:
                continue
        return steps

    @classmethod
    def list_speculators(
        cls,
    ) -> list[tuple[str, int, date | None, int]]:
        """List all speculative entry functions defined on this scraper.

        Introspects the class to find all methods decorated with
        @entry(speculative=True) and returns their metadata.

        Returns:
            List of tuples containing (name, highest_observed, observation_date, largest_observed_gap)
            for each speculative entry function.

        Example:
            >>> class MyScraper(BaseScraper[CaseData]):
            ...     @entry(CaseData, speculative=True, highest_observed=500, largest_observed_gap=20)
            ...     def fetch_case(self, case_id: int) -> NavigatingRequest:
            ...         return NavigatingRequest(...)
            ...
            >>> MyScraper.list_speculators()
            [('fetch_case', 500, None, 20)]
        """
        from kent.common.decorators import (
            get_entry_metadata,
        )

        speculators = []
        for name in dir(cls):
            if name.startswith("_"):
                continue
            try:
                method = getattr(cls, name)
                metadata = get_entry_metadata(method)
                if metadata is not None and metadata.speculative:
                    speculators.append(
                        (
                            name,
                            metadata.highest_observed,
                            metadata.observation_date,
                            metadata.largest_observed_gap,
                        )
                    )
            except Exception:
                continue
        return speculators

    @classmethod
    def list_entries(cls) -> list[EntryInfo]:
        """List all entry point methods defined on this scraper.

        Introspects the class to find all methods decorated with @entry
        and returns their metadata.

        Returns:
            List of EntryInfo objects for each decorated entry method.
        """
        from kent.common.decorators import (
            get_entry_metadata,
        )

        entries = []
        for name in dir(cls):
            if name.startswith("_"):
                continue
            try:
                method = getattr(cls, name)
                metadata = get_entry_metadata(method)
                if metadata is not None:
                    entries.append(
                        EntryInfo(
                            name=metadata.func_name,
                            return_type=metadata.return_type,
                            param_types=metadata.param_types,
                            speculative=metadata.speculative,
                            highest_observed=metadata.highest_observed,
                            largest_observed_gap=metadata.largest_observed_gap,
                        )
                    )
            except Exception:
                continue
        return entries

    def _list_entry_info(
        self,
    ) -> list[tuple[Any, Any]]:
        """List entry methods with their metadata for dispatch.

        Returns:
            List of (bound_method, EntryMetadata) tuples.
        """
        from kent.common.decorators import (
            get_entry_metadata,
        )

        entries = []
        for name in dir(self):
            if name.startswith("_"):
                continue
            try:
                method = getattr(self, name)
                metadata = get_entry_metadata(method)
                if metadata is not None:
                    entries.append((method, metadata))
            except Exception:
                continue
        return entries

    def initial_seed(
        self, params: list[dict[str, dict[str, Any]]]
    ) -> Generator[NavigatingRequest, None, None]:
        """Dispatch parameter list to entry functions and yield combined requests.

        Takes a JSON-serializable list of parameter invocations and dispatches
        them to the appropriate @entry functions.

        Args:
            params: List of single-key dicts mapping entry function name to kwargs.
                Example: [{"search_by_number": {"docket_number": "A10"}}]

        Yields:
            NavigatingRequest instances from each dispatched entry function.

        Raises:
            ValueError: If params is empty/None or references unknown entry names.
        """
        if not params:
            raise ValueError(
                "initial_seed() requires at least one parameter invocation"
            )

        entry_map = {
            info.func_name: (method, info)
            for method, info in self._list_entry_info()
        }

        for invocation in params:
            for func_name, kwargs_dict in invocation.items():
                if func_name not in entry_map:
                    available = list(entry_map.keys())
                    raise ValueError(
                        f"Unknown entry '{func_name}'. Available: {available}"
                    )
                method, meta = entry_map[func_name]
                validated_kwargs = meta.validate_params(kwargs_dict)
                yield from method(**validated_kwargs)

    @classmethod
    def schema(cls) -> dict[str, Any]:
        """Generate JSON Schema for all entry points.

        Returns a dict using Pydantic's model_json_schema() for BaseModel
        parameters and standard JSON Schema types for primitives.

        Returns:
            Dict with scraper name, entries, and $defs for referenced models.
        """
        from pydantic import BaseModel as PydanticBaseModel

        entries: dict[str, Any] = {}
        all_defs: dict[str, Any] = {}

        for entry_info in cls.list_entries():
            # Build parameter schema
            properties: dict[str, Any] = {}
            required: list[str] = []

            for param_name, param_type in entry_info.param_types.items():
                required.append(param_name)

                if isinstance(param_type, type) and issubclass(
                    param_type, PydanticBaseModel
                ):
                    # Use Pydantic's schema generation
                    pydantic_type = cast(type[PydanticBaseModel], param_type)
                    model_schema = pydantic_type.model_json_schema()
                    # Extract $defs and add to top-level
                    if "$defs" in model_schema:
                        all_defs.update(model_schema["$defs"])
                        del model_schema["$defs"]
                    # Store the model definition
                    type_name = param_type.__name__
                    all_defs[type_name] = model_schema
                    properties[param_name] = {"$ref": f"#/$defs/{type_name}"}
                elif param_type is str:
                    properties[param_name] = {"type": "string"}
                elif param_type is int:
                    properties[param_name] = {"type": "integer"}
                elif param_type is date:
                    properties[param_name] = {
                        "type": "string",
                        "format": "date",
                    }

            entry_schema: dict[str, Any] = {
                "returns": entry_info.return_type.__name__,
                "speculative": entry_info.speculative,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            }

            if entry_info.speculative:
                entry_schema["highest_observed"] = entry_info.highest_observed
                entry_schema["largest_observed_gap"] = (
                    entry_info.largest_observed_gap
                )

            entries[entry_info.name] = entry_schema

        result: dict[str, Any] = {
            "scraper": cls.__name__,
            "entries": entries,
        }
        if all_defs:
            result["$defs"] = all_defs

        return result

    def fails_successfully(self, response: Response) -> bool:
        """Detect hidden error states in successful HTTP responses.

        Some websites return HTTP 200 status codes but embed error states
        in the page content or headers (e.g., "No results found" pages,
        session timeout pages, soft 404s). This method allows scrapers to
        detect these hidden failures.

        This is primarily used for speculation handling. When a
        speculative request gets a 2xx response, the driver calls this
        method to check if the response actually represents a failure.
        If this returns False, the driver sets status_code=555 before
        calling the speculation callback.

        Args:
            response: The Response object to check for hidden errors.

        Returns:
            True if the response is genuinely successful (default behavior).
            False if the response contains a hidden error pattern.

        Example:
            Override this method to detect site-specific error patterns::

                def fails_successfully(self, response: Response) -> bool:
                    # Detect "No results" page that returns 200
                    if "No results found" in response.text:
                        return False
                    # Detect session timeout
                    if response.url.endswith("/login"):
                        return False
                    return True
        """
        return True


@dataclass(frozen=True)
class ParsedData(Generic[T]):
    """Data yielded by a scraper after parsing a page.

    This is a simple wrapper around a bit of returned data to enable exhaustive pattern
    matching in the driver. When a scraper yields data, it should wrap
    it in ParsedData so the driver can distinguish it from other yield
    types (like NavigatingRequest).

    Example:
        yield ParsedData({"docket": "BCC-2024-001", "case_name": "..."})
    """

    data: T
    __match_args__ = ("data",)

    def unwrap(self) -> T:
        return self.data


# =============================================================================
# Step 2: NavigatingRequest and Response
# =============================================================================
# Step 3: BaseRequest, NonNavigatingRequest, and current_location tracking
# Step 4: ArchiveRequest and ArchiveResponse for file downloads
# Step 5: accumulated_data with deep copy semantics


class HttpMethod(Enum):
    """HTTP methods supported by scrapers."""

    GET = "GET"
    OPTIONS = "OPTIONS"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"
    PATCH = "PATCH"
    HEAD = "HEAD"


# Type aliases for complex parameter types
QueryParams = dict[str, Any] | list[tuple[str, Any]] | bytes | None
RequestData = dict[str, Any] | list[tuple[str, Any]] | bytes | BinaryIO | None
HeadersType = dict[str, str] | None
CookiesType = dict[str, str] | CookieJar | None
FileTuple = (
    tuple[str, BinaryIO]
    | tuple[str, BinaryIO, str]
    | tuple[str, BinaryIO, str, dict[str, str]]
)
FilesType = dict[str, BinaryIO | FileTuple] | None
AuthType = tuple[str, str] | None
TimeoutType = float | tuple[float, float] | None
ProxiesType = dict[str, str] | None
VerifyType = bool | str
CertType = str | tuple[str, str] | None


@dataclass(frozen=True)
class HTTPRequestParams:
    """Parameters for an HTTP request, mirroring the requests library interface.

    :param method: HTTP method for the request: ``GET``, ``OPTIONS``, ``HEAD``,
        ``POST``, ``PUT``, ``PATCH``, or ``DELETE``.
    :param url: URL for the request.
    :param params: (optional) Dictionary, list of tuples or bytes to send
        in the query string for the request.
    :param data: (optional) Dictionary, list of tuples, bytes, or file-like
        object to send in the body of the request.
    :param json: (optional) A JSON serializable Python object to send in the
        body of the request.
    :param headers: (optional) Dictionary of HTTP Headers to send with the request.
    :param cookies: (optional) Dict or CookieJar object to send with the request.
    :param files: (optional) Dictionary of ``'name': file-like-objects``
        (or ``{'name': file-tuple}``) for multipart encoding upload.
        ``file-tuple`` can be a 2-tuple ``('filename', fileobj)``,
        3-tuple ``('filename', fileobj, 'content_type')``
        or a 4-tuple ``('filename', fileobj, 'content_type', custom_headers)``,
        where ``'content_type'`` is a string defining the content type of the
        given file and ``custom_headers`` a dict-like object containing
        additional headers to add for the file.
    :param auth: (optional) Auth tuple to enable Basic/Digest/Custom HTTP Auth.
    :param timeout: (optional) How many seconds to wait for the server to send
        data before giving up, as a float, or a (connect timeout, read timeout) tuple.
    :param allow_redirects: (optional) Boolean. Enable/disable
        GET/OPTIONS/POST/PUT/PATCH/DELETE/HEAD redirection. Defaults to ``True``.
    :param proxies: (optional) Dictionary mapping protocol to the URL of the proxy.
    :param verify: (optional) Either a boolean, in which case it controls whether
        we verify the server's TLS certificate, or a string, in which case it
        must be a path to a CA bundle to use. Defaults to ``True``.
    :param stream: (optional) if ``False``, the response content will be
        immediately downloaded.
    :param cert: (optional) if String, path to ssl client cert file (.pem).
        If Tuple, ('cert', 'key') pair.
    """

    method: HttpMethod
    url: str
    params: QueryParams = None
    data: RequestData = None
    json: Any = None
    headers: HeadersType = None
    cookies: CookiesType = None
    files: FilesType = None
    auth: AuthType = None
    timeout: TimeoutType = None
    allow_redirects: bool = True
    proxies: ProxiesType = None
    verify: VerifyType = True
    stream: bool = False
    cert: CertType = None


def _generate_deduplication_key(request_params: HTTPRequestParams) -> str:
    """Generate a deduplication key from HTTPRequestParams.

    Step 16: Default deduplication key is a SHA256 hash of:
    - Full URL with parameters
    - Request data (sorted if dict/list of tuples)

    Args:
        request_params: The HTTP request parameters.

    Returns:
        A SHA256 hex digest string for deduplication.
    """
    # Start with the full URL
    url_str = request_params.url

    # Add query parameters if present
    if request_params.params:
        # Sort params for consistent hashing
        if isinstance(request_params.params, dict):
            sorted_params = sorted(request_params.params.items())
            params_str = str(sorted_params)
        elif isinstance(request_params.params, list | tuple):
            # List of tuples
            sorted_params = sorted(request_params.params)  # type: ignore
            params_str = str(sorted_params)
        else:
            # bytes or other type - use as-is
            params_str = str(request_params.params)
        url_str = f"{url_str}?{params_str}"

    # Add request data if present
    data_str = ""
    if request_params.data:
        if isinstance(request_params.data, dict):
            # Sort dict by key
            sorted_data = sorted(request_params.data.items())
            data_str = str(sorted_data)
        elif isinstance(request_params.data, list):
            # Assume list of tuples, sort by first element
            sorted_data = sorted(
                request_params.data,
                key=lambda x: x[0] if isinstance(x, tuple) else x,
            )
            data_str = str(sorted_data)
        else:
            data_str = str(request_params.data)

    # Add JSON data if present
    if request_params.json is not None:
        if isinstance(request_params.json, dict):
            # Sort dict by key for consistent hashing
            json_str = json.dumps(request_params.json, sort_keys=True)
        else:
            json_str = json.dumps(request_params.json)
        data_str = f"{data_str}|{json_str}"

    # Combine URL and data, then hash
    combined = f"{url_str}|{data_str}"
    return hashlib.sha256(combined.encode("utf-8")).hexdigest()


class SkipDeduplicationCheck:
    """Skip deduplication checks."""

    pass


@dataclass(frozen=True)
class BaseRequest:
    """Base class for all request types.

    Provides common functionality for URL resolution and HTTP parameters.
    Each request tracks its current_location and request ancestry.

    Attributes:
        request: HTTP request parameters (URL, method, headers, etc.).
        continuation: The method name to call with the Response, or a Callable.
                     When a Callable is provided, the @step decorator will automatically
                     resolve it to the function's name.
        current_location: The URL context for resolving relative URLs.
        previous_requests: Chain of requests that led to this one.
        accumulated_data: Data collected across the request chain.
        aux_data: Navigation metadata (tokens, session data) needed for requests.
        priority: Priority for request queue ordering (lower = higher priority).
        deduplication_key: Key for deduplication (defaults to hash of URL and data).
        permanent: Persistent data (cookies, headers) that flows through the request chain.
        is_speculative: Whether this request is speculative (probing for content existence).
        speculation_id: Tuple of (function_name, integer_id) identifying which @speculate
                       function generated this request. None for non-speculative requests.
        via: Optional description of how the request was produced (ViaLink, ViaFormSubmit).
             Enables the Playwright driver to replay the browser action. HTTP driver ignores.
    """

    request: HTTPRequestParams
    continuation: str | Callable[..., Any]
    current_location: str = ""
    previous_requests: list[BaseRequest] = field(default_factory=list)
    accumulated_data: dict[str, Any] = field(default_factory=dict)
    aux_data: dict[str, Any] = field(default_factory=dict)
    priority: int = 9
    deduplication_key: str | None | SkipDeduplicationCheck = None
    permanent: dict[str, Any] = field(default_factory=dict)
    is_speculative: bool = False
    speculation_id: tuple[str, int] | None = None
    via: Any = None  # ViaLink | ViaFormSubmit | None - using Any to avoid circular import

    def __post_init__(self) -> None:
        """Deep copy accumulated_data, aux_data, and permanent to prevent unintended sharing.

        Step 16: Also generates default deduplication_key if not provided.
        Step 18: Also deep copies permanent dict and merges permanent headers/cookies
        into the HTTPRequestParams.

        When a scraper yields multiple requests from the same method, they might
        share the same accumulated_data or aux_data dicts. Without deep copy,
        mutations in one branch would affect sibling branches. This is critical
        for correctness.

        Example problem without deep copy::

            shared_data = {"case_name": "Ant v. Bee"}
            shared_aux = {"session_token": "abc123"}
            yield NavigatingRequest(url="/detail/1", accumulated_data=shared_data, aux_data=shared_aux)
            yield NavigatingRequest(url="/detail/2", accumulated_data=shared_data, aux_data=shared_aux)
            # If detail/1 mutates the dicts, detail/2 sees the mutation - BUG!

        The deep copy ensures each request gets its own independent copy of the data.
        """
        # Since the dataclass is frozen, we need to use object.__setattr__
        object.__setattr__(
            self, "accumulated_data", deepcopy(self.accumulated_data)
        )
        object.__setattr__(self, "aux_data", deepcopy(self.aux_data))
        object.__setattr__(self, "permanent", deepcopy(self.permanent))

        # Step 18: Merge permanent headers and cookies into HTTPRequestParams
        if self.permanent:
            new_request = self._merge_permanent_into_request()
            object.__setattr__(self, "request", new_request)

        # Step 16: Generate default deduplication key if not provided
        if self.deduplication_key is None:
            object.__setattr__(
                self,
                "deduplication_key",
                _generate_deduplication_key(self.request),
            )

    def _merge_permanent_into_request(self) -> HTTPRequestParams:
        """Merge permanent headers and cookies into the HTTPRequestParams.

        Returns:
            A new HTTPRequestParams with permanent data merged in.
        """
        req = self.request
        merged_headers: dict[str, str] | None = None
        # Merge headers
        if "headers" in self.permanent:
            merged_headers = dict(req.headers) if req.headers else {}
            merged_headers.update(self.permanent["headers"])
        else:
            merged_headers = req.headers

        # Merge cookies (only if both are dicts)
        if "cookies" in self.permanent:
            if req.cookies is None:
                merged_cookies: CookiesType = dict(self.permanent["cookies"])
            elif isinstance(req.cookies, dict):
                merged_cookies = dict(req.cookies)
                merged_cookies.update(self.permanent["cookies"])
            else:
                # CookieJar - can't merge, keep original
                merged_cookies = req.cookies
        else:
            merged_cookies = req.cookies

        return HTTPRequestParams(
            method=req.method,
            url=req.url,
            params=req.params,
            data=req.data,
            json=req.json,
            headers=merged_headers,
            cookies=merged_cookies,
            files=req.files,
            auth=req.auth,
            timeout=req.timeout,
            allow_redirects=req.allow_redirects,
            proxies=req.proxies,
            verify=req.verify,
            stream=req.stream,
            cert=req.cert,
        )

    def resolve_url(self, current_location: str) -> str:
        """Resolve the URL against the current location.

        Uses urllib.parse.urljoin to handle both relative and absolute URLs:
        - Absolute URLs (http://..., https://...) are returned unchanged
        - Relative URLs are resolved against current_location

        Args:
            current_location: The current page URL.

        Returns:
            The absolute URL.
        """
        # Normalize URL encoding
        parsed = urlparse(self.request.url)

        # Decode then encode to normalize (prevents double-encoding)
        # safe='/:?&=' preserves URL structure while encoding special chars
        decoded_path = unquote(parsed.path)
        encoded_path = quote(decoded_path, safe="/")

        decoded_query = unquote(parsed.query)
        encoded_query = quote(decoded_query, safe="=&")

        reencoded_url = urlunparse(
            (
                parsed.scheme,
                parsed.netloc,
                encoded_path,
                parsed.params,
                encoded_query,
                parsed.fragment,
            )
        )
        return urljoin(current_location, reencoded_url)

    def resolve_from(
        self, context: Response | NonNavigatingRequest
    ) -> BaseRequest:
        """Create a new request with URL resolved from a Response or NonNavigatingRequest.

        This method is overridden in NavigatingRequest and NonNavigatingRequest
        to provide specific behavior for each request type.

        Args:
            context: Response from a previous request or the originating NonNavigatingRequest.

        Returns:
            A new request with resolved URL and updated context.

        Raises:
            NotImplementedError: If called on BaseRequest directly.
        """
        raise NotImplementedError(
            "resolve_from must be implemented by subclasses"
        )

    def resolve_request_from(self, context: Response | BaseRequest):
        match context:
            case Response():
                # Response from a NavigatingRequest - use its URL
                resolved_location = context.url
                parent_request = context.request
            case BaseRequest():
                # NonNavigatingRequest - use its current_location
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

    def speculative(self, func_name: str, spec_id: int) -> BaseRequest:
        """Create a speculative copy of this request.

        Only NavigatingRequest supports speculative requests. This method
        raises NotImplementedError for other request types.

        Args:
            func_name: Name of the @speculate function generating this request.
            spec_id: The integer ID passed to the @speculate function.

        Returns:
            A copy of the request with is_speculative=True and speculation_id set.

        Raises:
            NotImplementedError: Only NavigatingRequest can be speculative.
        """
        raise NotImplementedError(
            f"Only NavigatingRequest can be speculative. "
            f"{type(self).__name__} does not support speculation."
        )


@dataclass(frozen=True)
class NavigatingRequest(BaseRequest):
    """A request to navigate to a new page.

    When a scraper yields a NavigatingRequest, the driver will:
    1. Fetch the URL (resolving relative URLs against current_location)
    2. Update current_location to the new URL
    3. Call the continuation method with the Response

    The continuation is specified as a string (method name) rather than
    a function reference, making requests fully serializable for persistence.

    This differs from NonNavigatingRequest which fetches data without
    updating current_location (useful for API calls).
    """

    def resolve_from(
        self, context: Response | NonNavigatingRequest
    ) -> NavigatingRequest:
        """Create a new request with URL resolved from a Response or NonNavigatingRequest.

        For NavigatingRequest:
        - If context is a Response, use the response's URL as current_location
        - If context is a NonNavigatingRequest, use its current_location
        - accumulated_data and aux_data are carried forward from the new request (self)

        Args:
            context: Response from a NavigatingRequest or the originating NonNavigatingRequest.

        Returns:
            A new NavigatingRequest with resolved URL and updated context.
        """
        request, location, parent = self.resolve_request_from(context)
        # Step 18: Merge permanent data - parent's permanent + this request's permanent
        merged_permanent = {**parent.permanent, **self.permanent}
        return NavigatingRequest(
            request=request,
            continuation=self.continuation,
            current_location=location,
            previous_requests=parent.previous_requests + [parent],
            accumulated_data=self.accumulated_data,
            aux_data=self.aux_data,
            priority=self.priority,
            deduplication_key=self.deduplication_key,
            permanent=merged_permanent,
            is_speculative=self.is_speculative,
            speculation_id=self.speculation_id,
            via=self.via,
        )

    def speculative(self, func_name: str, spec_id: int) -> NavigatingRequest:
        """Create a speculative copy of this request.

        Returns a new NavigatingRequest with is_speculative=True and
        speculation_id set to (func_name, spec_id).

        Args:
            func_name: Name of the @speculate function generating this request.
            spec_id: The integer ID passed to the @speculate function.

        Returns:
            A new NavigatingRequest with speculation fields set.
        """
        return NavigatingRequest(
            request=self.request,
            continuation=self.continuation,
            current_location=self.current_location,
            previous_requests=self.previous_requests,
            accumulated_data=self.accumulated_data,
            aux_data=self.aux_data,
            priority=self.priority,
            deduplication_key=self.deduplication_key,
            permanent=self.permanent,
            is_speculative=True,
            speculation_id=(func_name, spec_id),
            via=self.via,
        )


@dataclass(frozen=True)
class NonNavigatingRequest(BaseRequest):
    """A request that fetches data without changing the current location.

    When a scraper yields a NonNavigatingRequest, the driver will:
    1. Fetch the URL (resolving relative URLs against current_location)
    2. Keep current_location unchanged
    3. Call the continuation method with the Response

    This is useful for API calls that provide supplementary data without
    navigating away from the current page. For example, fetching JSON
    metadata from an API while staying on an HTML detail page.

    The continuation is specified as a string (method name) for serializability.
    """

    def resolve_from(
        self, context: Response | NonNavigatingRequest
    ) -> NonNavigatingRequest:
        """Create a new request with URL resolved from a Response or NonNavigatingRequest.

        For NonNavigatingRequest:
        - If context is a Response, use the response's URL as current_location
        - If context is a NonNavigatingRequest, use its current_location
        - current_location stays unchanged (inherited from parent)
        - accumulated_data and aux_data are carried forward from the new request (self)

        Args:
            context: Response from a NavigatingRequest or the originating NonNavigatingRequest.

        Returns:
            A new NonNavigatingRequest with resolved URL and preserved current_location.
        """
        request, location, parent = self.resolve_request_from(context)
        # Step 18: Merge permanent data - parent's permanent + this request's permanent
        merged_permanent = {**parent.permanent, **self.permanent}
        return NonNavigatingRequest(
            request=request,
            continuation=self.continuation,
            current_location=location,
            previous_requests=parent.previous_requests + [parent],
            accumulated_data=self.accumulated_data,
            aux_data=self.aux_data,
            priority=self.priority,
            deduplication_key=self.deduplication_key,
            permanent=merged_permanent,
            via=self.via,
        )


@dataclass(frozen=True)
class ArchiveRequest(NonNavigatingRequest):
    """A request to download and archive a file.

    When a scraper yields an ArchiveRequest, the driver will:
    1. Fetch the URL (resolving relative URLs against current_location)
    2. Download the file content
    3. Save it to local storage
    4. Call the continuation method with an ArchiveResponse

    This is useful for downloading binary files like PDFs, MP3s, images, etc.
    The ArchiveResponse includes a file_url field with the local storage path.

    Like NonNavigatingRequest, ArchiveRequest preserves current_location -
    downloading a file doesn't change where you are in the scraper's navigation.

    Attributes:
        expected_type: Optional hint about the file type ("pdf", "audio", etc.).
        priority: Priority for request queue ordering (default 1, higher priority than regular requests).
    """

    expected_type: str | None = None
    priority: int = 1

    def resolve_from(
        self, context: Response | NonNavigatingRequest
    ) -> ArchiveRequest:
        """Create a new request with URL resolved from a Response or NonNavigatingRequest.

        For ArchiveRequest (like NonNavigatingRequest):
        - If context is a Response, use the response's URL as current_location
        - If context is a NonNavigatingRequest, use its current_location
        - current_location stays unchanged (inherited from parent)
        - accumulated_data and aux_data are carried forward from the new request (self)

        Args:
            context: Response from a NavigatingRequest or the originating NonNavigatingRequest.

        Returns:
            A new ArchiveRequest with resolved URL and preserved current_location.
        """
        request, location, parent = self.resolve_request_from(context)
        # Step 18: Merge permanent data - parent's permanent + this request's permanent
        merged_permanent = {**parent.permanent, **self.permanent}
        return ArchiveRequest(
            request=request,
            continuation=self.continuation,
            current_location=location,
            previous_requests=parent.previous_requests + [parent],
            expected_type=self.expected_type,
            accumulated_data=self.accumulated_data,
            aux_data=self.aux_data,
            priority=self.priority,
            deduplication_key=self.deduplication_key,
            permanent=merged_permanent,
            via=self.via,
        )


@dataclass
class Response:
    """HTTP response from fetching a page.

    Modeled after httpx.Response to provide a familiar interface.
    The driver creates Response objects and passes them to scraper
    continuation methods.

    Attributes:
        status_code: HTTP status code (200, 404, etc.).
        headers: Response headers.
        content: Raw response bytes.
        text: Decoded response text.
        url: Final URL after any redirects.
        request: The BaseRequest that triggered this response.
    """

    status_code: int
    headers: dict[str, str]
    content: bytes
    text: str
    url: str
    request: BaseRequest


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


# =============================================================================
# Type Alias for Scraper Yields
# =============================================================================

# A scraper can yield ParsedData, NavigatingRequest, NonNavigatingRequest,
# ArchiveRequest, or None.
# This type alias enables exhaustive pattern matching in the driver.
ScraperYield = (
    ParsedData[T]
    | NavigatingRequest
    | NonNavigatingRequest
    | ArchiveRequest
    | None
)

# Type alias for scraper generator - what continuation methods return
# The second type parameter (bool | None) is the SendType - values sent back
# to the generator via .send(). Currently unused but kept for future compatibility.
ScraperGenerator = Generator[ScraperYield[T], bool | None, None]


# =============================================================================
# Wait Conditions for Playwright Driver
# =============================================================================


@dataclass(frozen=True)
class WaitForSelector:
    """Wait for a selector to appear in the DOM.

    Used in @step(await_list=[...]) to instruct Playwright driver
    to wait for an element before taking a DOM snapshot.

    Attributes:
        selector: CSS or XPath selector to wait for.
        state: Optional state to wait for ('attached', 'detached', 'visible', 'hidden').
               Defaults to 'visible'.
        timeout: Optional timeout in milliseconds. If None, uses Playwright's default.
    """

    selector: str
    state: str = "visible"
    timeout: int | None = None


@dataclass(frozen=True)
class WaitForLoadState:
    """Wait for a specific load state.

    Used in @step(await_list=[...]) to instruct Playwright driver
    to wait for a load state before taking a DOM snapshot.

    Attributes:
        state: Load state to wait for ('load', 'domcontentloaded', 'networkidle').
        timeout: Optional timeout in milliseconds. If None, uses Playwright's default.
    """

    state: str = "load"
    timeout: int | None = None


@dataclass(frozen=True)
class WaitForURL:
    """Wait for the URL to match a pattern.

    Used in @step(await_list=[...]) to instruct Playwright driver
    to wait for URL navigation before taking a DOM snapshot.

    Attributes:
        url: URL string or pattern to wait for. Can be a string, regex pattern, or callable.
        timeout: Optional timeout in milliseconds. If None, uses Playwright's default.
    """

    url: str
    timeout: int | None = None


@dataclass(frozen=True)
class WaitForTimeout:
    """Wait for a specific amount of time.

    Used in @step(await_list=[...]) to instruct Playwright driver
    to wait before taking a DOM snapshot.

    Attributes:
        timeout: Time to wait in milliseconds.
    """

    timeout: int
