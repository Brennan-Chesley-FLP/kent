"""Playwright driver implementation for JavaScript-heavy websites.

This driver extends LocalDevDriver to handle JavaScript-heavy court websites
using Playwright browser automation. It maintains step function purity by:

1. Rendering pages in a real browser
2. Serializing the rendered DOM to HTML
3. Parsing HTML with LXML and injecting as PageElement
4. Never passing live browser references to step functions

Key features:
- DOM snapshot model for step function purity
- Via handling for form submission and navigation replay
- Await list for explicit wait conditions before snapshot
- Autowait for automatic retry on element query failures
- Incidental requests tracking for browser-initiated network activity
- Rate limiting via pyrate_limiter
- Browser lifecycle management with context persistence
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from playwright.async_api import (
    Browser,
    BrowserContext,
    Page,
    async_playwright,
)
from playwright.async_api import (
    TimeoutError as PlaywrightTimeoutError,
)
from pyrate_limiter import Limiter

from kent.common.decorators import get_step_metadata
from kent.common.exceptions import (
    HTMLStructuralAssumptionException,
    TransientException,
)
from kent.common.page_element import (
    ViaFormSubmit,
    ViaLink,
)
from kent.common.selector_observer import (
    SelectorObserver,
    SelectorQuery,
)
from kent.data_types import (
    BaseRequest,
    BaseScraper,
    Response,
    WaitForLoadState,
    WaitForSelector,
    WaitForTimeout,
    WaitForURL,
)
from kent.driver.persistent_driver.compression import (
    compress,
)
from kent.driver.persistent_driver.persistent_driver import (
    PersistentDriver,
)
from kent.driver.persistent_driver.rate_limiter import (
    AioSQLiteBucket,
)
from kent.driver.persistent_driver.sql_manager import (
    SQLManager,
)

if TYPE_CHECKING:
    from collections.abc import AsyncIterator, Callable

    from kent.data_types import BaseRequest

logger = logging.getLogger(__name__)

ScraperReturnDatatype = TypeVar("ScraperReturnDatatype")


class PlaywrightDriver(
    PersistentDriver[ScraperReturnDatatype], Generic[ScraperReturnDatatype]
):
    """Playwright-based driver for JavaScript-heavy court websites.

    Extends LocalDevDriver to use browser automation instead of HTTP requests.
    Maintains step function purity through DOM snapshotting.

    Args:
        scraper: The scraper instance to run.
        db: SQLManager for database operations.
        browser_context: Playwright browser context for navigations.
        storage_dir: Directory for downloaded files.
        num_workers: Number of initial concurrent workers (default: 1).
        max_workers: Maximum workers for dynamic scaling (default: 10).
        resume: If True, resume from existing queue state (default: True).
        max_backoff_time: Maximum total backoff time before marking failed (default: 3600.0).
        request_manager: AsyncRequestManager for handling HTTP requests.
        enable_monitor: If True (default), start the worker monitor for dynamic scaling.

    Example:
        async with PlaywrightDriver.open(scraper, db_path) as driver:
            driver.on_progress = lambda e: print(e.to_json())
            await driver.run()
    """

    def __init__(
        self,
        scraper: BaseScraper[ScraperReturnDatatype],
        db: SQLManager,
        browser_context: BrowserContext,
        storage_dir: Path | None = None,
        num_workers: int = 1,
        max_workers: int = 10,
        resume: bool = True,
        max_backoff_time: float = 3600.0,
        request_manager: Any | None = None,
        enable_monitor: bool = True,
        excluded_resource_types: set[str] | None = None,
        rate_limiter: Limiter | None = None,
    ) -> None:
        """Initialize the Playwright driver.

        Note: Use PlaywrightDriver.open() for proper async initialization.

        Args:
            scraper: The scraper instance to run.
            db: SQLManager for database operations.
            browser_context: Playwright browser context for navigations.
            storage_dir: Directory for downloaded files.
            num_workers: Number of initial concurrent workers.
            max_workers: Maximum workers for dynamic scaling.
            resume: If True, resume from existing queue state.
            max_backoff_time: Maximum total backoff time before marking failed.
            request_manager: AsyncRequestManager for handling HTTP requests.
            enable_monitor: If True (default), start the worker monitor for dynamic scaling.
            excluded_resource_types: Resource types to exclude from content capture (default: {"image", "media", "font"}).
            rate_limiter: Optional rate limiter for controlling navigation pace.
        """
        super().__init__(
            scraper=scraper,
            db=db,
            storage_dir=storage_dir,
            num_workers=num_workers,
            max_workers=max_workers,
            resume=resume,
            max_backoff_time=max_backoff_time,
            request_manager=request_manager,
            enable_monitor=enable_monitor,
        )

        self.browser_context = browser_context
        # Page reuse within context for sequential navigations
        self._page: Page | None = None
        # Track incidental requests for current navigation
        self._incidental_requests: list[dict[str, Any]] = []
        # Current parent request ID for linking incidental requests
        self._current_parent_request_id: int | None = None
        # Resource types to exclude from content capture
        self.excluded_resource_types = excluded_resource_types or {
            "image",
            "media",
            "font",
        }
        # Rate limiter for controlling navigation pace
        self.rate_limiter = rate_limiter

    @classmethod
    @asynccontextmanager
    async def open(
        cls,
        scraper: BaseScraper[ScraperReturnDatatype],
        db_path: Path,
        browser_type: str = "chromium",
        headless: bool = True,
        viewport: dict[str, int] | None = None,
        user_agent: str | None = None,
        locale: str = "en-US",
        timezone_id: str = "America/New_York",
        **kwargs: Any,
    ) -> AsyncIterator[PlaywrightDriver[ScraperReturnDatatype]]:
        """Open Playwright driver as async context manager.

        Ensures proper initialization and cleanup of browser and DB connections.

        Args:
            scraper: The scraper instance to run.
            db_path: Path to SQLite database file.
            browser_type: Browser type: "chromium", "firefox", or "webkit" (default: "chromium").
            headless: Run browser in headless mode (default: True).
            viewport: Browser viewport size {"width": 1280, "height": 720} (default: None = 1280x720).
            user_agent: Custom user agent string (default: None = browser default).
            locale: Browser locale (default: "en-US").
            timezone_id: Browser timezone (default: "America/New_York").
            **kwargs: Additional arguments passed to __init__.

        Yields:
            Initialized PlaywrightDriver instance.

        Example:
            async with PlaywrightDriver.open(
                scraper,
                Path("run.db"),
                browser_type="chromium",
                headless=True,
            ) as driver:
                await driver.run()
        """
        # Extract driver-specific kwargs
        storage_dir = kwargs.pop("storage_dir", None)
        num_workers = kwargs.pop("num_workers", 1)
        max_workers = kwargs.pop("max_workers", 10)
        resume = kwargs.pop("resume", True)
        max_backoff_time = kwargs.pop("max_backoff_time", 3600.0)
        enable_monitor = kwargs.pop("enable_monitor", True)
        excluded_resource_types = kwargs.pop("excluded_resource_types", None)
        rates = kwargs.pop("rates", None)

        # Default viewport
        if viewport is None:
            viewport = {"width": 1280, "height": 720}

        # Check if we're resuming and should load browser config from DB
        stored_browser_config = None
        if resume and db_path.exists():
            # Temporarily open DB to check for stored config
            async with SQLManager.open(db_path) as temp_db:
                run_metadata = await temp_db.get_run_metadata()
                if run_metadata and run_metadata.get("browser_config"):
                    stored_browser_config = run_metadata["browser_config"]

        # Use stored config if resuming, otherwise create new config
        if stored_browser_config:
            browser_config = stored_browser_config
            # Extract values from stored config
            browser_type = browser_config.get("browser_type", browser_type)
            headless = browser_config.get("headless", headless)
            viewport = browser_config.get("viewport", viewport)
            user_agent = browser_config.get("user_agent", user_agent)
            locale = browser_config.get("locale", locale)
            timezone_id = browser_config.get("timezone_id", timezone_id)
        else:
            # Create new browser configuration for persistence
            browser_config = {
                "browser_type": browser_type,
                "headless": headless,
                "viewport": viewport,
                "user_agent": user_agent,
                "locale": locale,
                "timezone_id": timezone_id,
            }

        # Initialize database and SQLManager
        from kent.driver.persistent_driver.database import (
            init_database,
        )

        engine, session_factory = await init_database(db_path)
        db = SQLManager(engine, session_factory)

        try:
            # Initialize run metadata with browser config
            # scraper_name is the full module path for debugger's compare command
            scraper_name = scraper.__class__.__module__
            scraper_version = getattr(scraper, "__version__", None)
            await db.init_run_metadata(
                scraper_name=scraper_name,
                scraper_version=scraper_version,
                num_workers=num_workers,
                max_backoff_time=max_backoff_time,
                browser_config=browser_config,
            )

            # Restore queue if resuming
            if resume:
                await db.restore_queue()

            # Initialize Playwright
            playwright = await async_playwright().start()
            try:
                # Launch browser
                browser_launcher = getattr(playwright, browser_type)
                browser: Browser = await browser_launcher.launch(
                    headless=headless
                )

                try:
                    # Create browser context with configuration
                    context_kwargs: dict[str, Any] = {
                        "viewport": viewport,
                        "locale": locale,
                        "timezone_id": timezone_id,
                    }
                    if user_agent:
                        context_kwargs["user_agent"] = user_agent

                    browser_context = await browser.new_context(
                        **context_kwargs
                    )

                    try:
                        # Initialize rate limiter from explicit rates or scraper declaration
                        rate_limiter = None
                        effective_rates = rates or scraper.rate_limits
                        if effective_rates:
                            bucket = AioSQLiteBucket(
                                db._session_factory, effective_rates
                            )
                            rate_limiter = Limiter(bucket)

                        # Create driver instance (no request manager needed for Playwright)
                        driver = cls(
                            scraper=scraper,
                            db=db,
                            browser_context=browser_context,
                            storage_dir=storage_dir,
                            num_workers=num_workers,
                            max_workers=max_workers,
                            resume=resume,
                            max_backoff_time=max_backoff_time,
                            request_manager=None,  # Playwright doesn't use HTTP request manager
                            enable_monitor=enable_monitor,
                            excluded_resource_types=excluded_resource_types,
                            rate_limiter=rate_limiter,
                        )

                        yield driver

                        # Close driver (persist state)
                        await driver.close()

                    finally:
                        # Close browser context
                        await browser_context.close()

                finally:
                    # Close browser
                    await browser.close()

            finally:
                # Stop Playwright
                await playwright.stop()

        finally:
            # Close database connection
            await engine.dispose()

    async def _process_regular_request(
        self,
        request_id: int,
        request: BaseRequest,
        continuation_name: str,
    ) -> None:
        """Process a request using Playwright navigation.

        Overrides LocalDevDriver's HTTP-based request processing to use browser automation.

        Args:
            request_id: Database ID of the request.
            request: The request to process.
            continuation_name: The continuation method name to invoke after navigation.
        """
        self._current_parent_request_id = request_id

        try:
            # Acquire rate limiter token before navigation
            if self.rate_limiter:
                await self.rate_limiter.try_acquire_async(
                    name="navigation", weight=1
                )

            # Get or create page
            if self._page is None:
                self._page = await self.browser_context.new_page()
                # Register network listeners for incidental request tracking
                self._register_network_listeners(self._page)

            # Clear incidental requests from previous navigation
            self._incidental_requests.clear()

            # Execute navigation based on via field
            if request.via is not None:
                await self._execute_via_navigation(request)
            else:
                # Direct URL navigation
                await self._page.goto(
                    request.request.url, wait_until="domcontentloaded"
                )

            # Process await_list if continuation has one
            if continuation_name:
                continuation = getattr(self.scraper, continuation_name, None)
                if continuation:
                    metadata = get_step_metadata(continuation)
                    if metadata and metadata.await_list:
                        await self._process_await_list(
                            self._page, metadata.await_list
                        )

            # Snapshot DOM
            html_content = await self._page.content()

            # Create Response object
            response = Response(
                status_code=200,  # Playwright doesn't expose status code directly
                url=self._page.url,
                content=html_content.encode("utf-8"),
                text=html_content,
                headers={},
                request=request,
            )

            # Store response with DOM snapshot
            await self._store_response(
                request_id=request_id,
                response=response,
                continuation=continuation_name,
                speculation_outcome=None,
            )

            # Store incidental requests
            for incidental in self._incidental_requests:
                await self.db.insert_incidental_request(
                    parent_request_id=request_id, **incidental
                )

            # Process continuation if present
            if continuation_name:
                continuation = getattr(self.scraper, continuation_name, None)
                if continuation:
                    # Check for autowait
                    metadata = get_step_metadata(continuation)
                    auto_await_timeout = (
                        metadata.auto_await_timeout if metadata else None
                    )

                    if auto_await_timeout:
                        # Process with autowait retry logic
                        await self._process_generator_with_autowait(
                            continuation,
                            response,
                            request,
                            request_id,
                            auto_await_timeout,
                        )
                    else:
                        # Process normally
                        gen = continuation(response)
                        await self._process_generator_with_storage(
                            gen,
                            response,
                            request,
                            continuation_name,
                            request_id,
                        )

            # Mark request as completed
            await self.db.mark_request_completed(request_id)

        except PlaywrightTimeoutError as e:
            # Timeout waiting for selector/load state
            logger.warning(f"Playwright timeout for request {request_id}: {e}")
            raise TransientException(f"Playwright timeout: {e}") from e

        except HTMLStructuralAssumptionException:
            # Structural failure - will be handled by autowait if enabled
            raise

        except Exception as e:
            logger.error(
                f"Error processing Playwright request {request_id}: {e}",
                exc_info=True,
            )
            raise

        finally:
            self._current_parent_request_id = None

    async def _execute_via_navigation(self, request: BaseRequest) -> None:
        """Execute browser navigation based on via field.

        Args:
            request: The request with via field (ViaFormSubmit or ViaLink).

        Raises:
            HTMLStructuralAssumptionException: If selector doesn't match in live DOM.
        """
        assert self._page is not None, (
            "Page must be initialized before navigation"
        )

        if isinstance(request.via, ViaFormSubmit):
            # Form submission
            form_via = request.via
            try:
                # Locate form
                form_element = await self._page.wait_for_selector(
                    form_via.form_selector, timeout=5000
                )
                if not form_element:
                    raise HTMLStructuralAssumptionException(
                        selector=form_via.form_selector,
                        selector_type="form",
                        description=f"Form selector not found: {form_via.form_selector}",
                        expected_min=1,
                        expected_max=1,
                        actual_count=0,
                        request_url=request.request.url,
                    )

                # Fill form fields
                for field_name, field_value in form_via.field_data.items():
                    # Locate field relative to form
                    field_selector = f'[name="{field_name}"]'
                    field_element = await form_element.query_selector(
                        field_selector
                    )
                    if field_element:
                        await field_element.fill(str(field_value))

                # Click submit button
                if form_via.submit_selector:
                    submit_element = await form_element.query_selector(
                        form_via.submit_selector
                    )
                    if not submit_element:
                        raise HTMLStructuralAssumptionException(
                            selector=form_via.submit_selector,
                            selector_type="submit",
                            description=f"Submit selector not found: {form_via.submit_selector}",
                            expected_min=1,
                            expected_max=1,
                            actual_count=0,
                            request_url=request.request.url,
                        )
                    # Wait for navigation after click
                    async with self._page.expect_navigation():
                        await submit_element.click()
                else:
                    # Click first submit-type element
                    submit_element = await form_element.query_selector(
                        'button[type="submit"], input[type="submit"]'
                    )
                    if not submit_element:
                        raise HTMLStructuralAssumptionException(
                            selector=form_via.form_selector,
                            selector_type="form",
                            description="No submit button found in form",
                            expected_min=1,
                            expected_max=1,
                            actual_count=0,
                            request_url=request.request.url,
                        )
                    async with self._page.expect_navigation():
                        await submit_element.click()

            except PlaywrightTimeoutError as e:
                raise HTMLStructuralAssumptionException(
                    selector=form_via.form_selector,
                    selector_type="form",
                    description=f"Selector timeout: {form_via.form_selector}",
                    expected_min=1,
                    expected_max=1,
                    actual_count=0,
                    request_url=request.request.url,
                ) from e

        elif isinstance(request.via, ViaLink):
            # Link navigation
            link_via = request.via
            try:
                link_element = await self._page.wait_for_selector(
                    link_via.selector, timeout=5000
                )
                if not link_element:
                    raise HTMLStructuralAssumptionException(
                        selector=link_via.selector,
                        selector_type="link",
                        description=f"Link selector not found: {link_via.selector}",
                        expected_min=1,
                        expected_max=1,
                        actual_count=0,
                        request_url=request.request.url,
                    )

                # Click link and wait for navigation
                async with self._page.expect_navigation():
                    await link_element.click()

            except PlaywrightTimeoutError as e:
                raise HTMLStructuralAssumptionException(
                    selector=link_via.selector,
                    selector_type="link",
                    description=f"Link selector timeout: {link_via.selector}",
                    expected_min=1,
                    expected_max=1,
                    actual_count=0,
                    request_url=request.request.url,
                ) from e

        else:
            # Direct URL navigation (no via)
            await self._page.goto(
                request.request.url, wait_until="domcontentloaded"
            )

    async def _process_await_list(
        self, page: Page, await_list: list[Any]
    ) -> None:
        """Process await_list wait conditions before taking DOM snapshot.

        Args:
            page: The Playwright page to wait on.
            await_list: List of wait condition objects.

        Raises:
            TransientException: If a wait condition times out.
        """
        for condition in await_list:
            try:
                if isinstance(condition, WaitForSelector):
                    await page.wait_for_selector(
                        condition.selector,
                        state=condition.state,
                        timeout=condition.timeout,
                    )

                elif isinstance(condition, WaitForLoadState):
                    await page.wait_for_load_state(
                        condition.state, timeout=condition.timeout
                    )

                elif isinstance(condition, WaitForURL):
                    await page.wait_for_url(
                        condition.url, timeout=condition.timeout
                    )

                elif isinstance(condition, WaitForTimeout):
                    await asyncio.sleep(condition.timeout / 1000.0)

                else:
                    logger.warning(
                        f"Unknown wait condition type: {type(condition)}"
                    )

            except PlaywrightTimeoutError as e:
                raise TransientException(
                    f"Wait condition timeout: {condition}"
                ) from e

    async def _process_generator_with_autowait(
        self,
        continuation: Callable,
        response: Response,
        parent_request: BaseRequest,
        request_id: int,
        auto_await_timeout: int,
    ) -> None:
        """Process generator with autowait retry logic.

        Args:
            continuation: The step function to invoke.
            response: The response to pass to the step function.
            parent_request: The parent request.
            request_id: The database request ID.
            auto_await_timeout: Timeout in milliseconds for autowait retries.
        """
        assert self._page is not None, "Page must be initialized for autowait"
        start_time = time.time()
        timeout_seconds = auto_await_timeout / 1000.0

        while True:
            try:
                # Try to process the generator
                gen = continuation(response)
                await self._process_generator_with_storage(
                    gen,
                    response,
                    parent_request,
                    continuation.__name__,
                    request_id,
                )
                # Success - exit retry loop
                break

            except HTMLStructuralAssumptionException as e:
                # Check if we've exhausted timeout
                elapsed = time.time() - start_time
                if elapsed >= timeout_seconds:
                    logger.warning(
                        f"Autowait timeout exhausted ({auto_await_timeout}ms) for request {request_id}"
                    )
                    raise

                # Check if selector is Playwright-compatible
                if not self._is_playwright_compatible_selector(
                    e.selector, e.selector_type
                ):
                    logger.debug(
                        f"Selector not Playwright-compatible, skipping autowait: {e.selector}"
                    )
                    raise

                # Get observer from last execution (if available)
                metadata = get_step_metadata(continuation)
                observer = metadata.observer if metadata else None

                # Compose absolute selector
                if observer and e.selector:
                    absolute_selector = self._compose_absolute_selector(
                        e.selector, observer
                    )
                else:
                    absolute_selector = e.selector

                logger.info(
                    f"Autowait: waiting for selector {absolute_selector}"
                )

                # Wait for selector in live browser
                remaining_timeout = int(
                    (timeout_seconds - elapsed) * 1000
                )  # Convert to ms
                try:
                    await self._page.wait_for_selector(
                        absolute_selector, timeout=remaining_timeout
                    )
                except PlaywrightTimeoutError:
                    logger.warning(
                        f"Autowait failed: selector {absolute_selector} not found within timeout"
                    )
                    raise e from None  # Re-raise original exception

                # Re-snapshot DOM
                html_content = await self._page.content()
                response = Response(
                    status_code=response.status_code,
                    url=self._page.url,
                    content=html_content.encode("utf-8"),
                    text=html_content,
                    headers=response.headers,
                    request=response.request,
                )

                # Update stored response
                await self._store_response(
                    request_id=request_id,
                    response=response,
                    continuation=continuation.__name__,
                    speculation_outcome=None,
                )

                # Retry step function with fresh DOM
                logger.info(
                    "Autowait: retrying step function with fresh DOM snapshot"
                )

    def _is_playwright_compatible_selector(
        self, selector: str, selector_type: str | None
    ) -> bool:
        """Check if selector is compatible with Playwright wait_for_selector.

        Args:
            selector: The XPath or CSS selector.
            selector_type: The type of selector (xpath, css, etc).

        Returns:
            True if compatible, False otherwise.
        """
        # Check for non-element XPath nodes
        if selector.endswith("/text()") or selector.endswith("/@"):
            return False

        # Check for EXSLT extensions
        if any(
            prefix in selector
            for prefix in ["re:", "str:", "math:", "set:", "dyn:"]
        ):
            return False

        # Check for XPath variables
        return "$" not in selector

    def _compose_absolute_selector(
        self, selector: str, observer: SelectorObserver
    ) -> str:
        """Compose absolute selector from relative selector and observer.

        Args:
            selector: The relative selector that failed.
            observer: The SelectorObserver with query tree.

        Returns:
            Absolute selector composed from parent chain.
        """
        # If already absolute, return as-is
        if selector.startswith("//") or selector.startswith("/"):
            return selector

        # Find the query in the observer's tree that matches this selector
        query = self._find_query_by_selector(observer.queries, selector)
        if not query:
            # No matching query found, return selector as-is
            return selector

        # Build path by walking up the parent chain
        path_parts: list[str] = []
        current: SelectorQuery | None = query
        while current:
            path_parts.append(current.selector)
            current = current.parent

        # Reverse to get root-to-leaf order
        path_parts.reverse()

        # Compose absolute XPath by joining parts
        # If the selector is relative (starts with .), we need to compose properly
        if path_parts and path_parts[0].startswith("//"):
            # First part is already absolute, join rest
            result = path_parts[0]
            for part in path_parts[1:]:
                if part.startswith(".//"):
                    # Descendant: replace .// with //
                    result = result + "//" + part[3:]
                elif part.startswith("./"):
                    # Child: replace ./ with /
                    result = result + "/" + part[2:]
                elif part.startswith("."):
                    # Self or relative
                    result = result + "/" + part[1:]
                else:
                    # Shouldn't happen but handle gracefully
                    result = result + "/" + part
            return result
        else:
            # Fallback: join with /
            return "/".join(path_parts)

    def _find_query_by_selector(
        self, queries: list, selector: str
    ) -> SelectorQuery | None:
        """Find a SelectorQuery in the tree matching the given selector.

        Args:
            queries: List of SelectorQuery objects to search.
            selector: The selector string to find.

        Returns:
            The matching SelectorQuery, or None if not found.
        """
        for query in queries:
            if query.selector == selector:
                return query
            # Recursively search children
            found = self._find_query_by_selector(query.children, selector)
            if found:
                return found
        return None

    def _register_network_listeners(self, page: Page) -> None:
        """Register network request/response listeners for incidental tracking.

        Args:
            page: The Playwright page to listen on.
        """

        async def on_request(request):
            """Capture request metadata."""
            # Store request info (will be updated with response when it arrives)
            incidental = {
                "resource_type": request.resource_type,
                "method": request.method,
                "url": request.url,
                "headers_json": json.dumps(dict(request.headers)),
                "body": None,  # Playwright doesn't expose request body easily
                "status_code": None,
                "response_headers_json": None,
                "content_compressed": None,
                "content_size_original": None,
                "content_size_compressed": None,
                "compression_dict_id": None,
                "started_at_ns": time.time_ns(),
                "completed_at_ns": None,
                "from_cache": None,
                "failure_reason": None,
            }
            self._incidental_requests.append(incidental)

        async def on_response(response):
            """Capture response metadata and content."""
            # Find corresponding request
            request = response.request
            for incidental in self._incidental_requests:
                if (
                    incidental["url"] == request.url
                    and incidental["completed_at_ns"] is None
                ):
                    # Update with response info
                    incidental["status_code"] = response.status
                    incidental["response_headers_json"] = json.dumps(
                        dict(response.headers)
                    )
                    incidental["completed_at_ns"] = time.time_ns()
                    incidental["from_cache"] = response.from_service_worker

                    # Capture content for certain resource types (skip large binaries)
                    if (
                        incidental["resource_type"]
                        not in self.excluded_resource_types
                    ):
                        try:
                            content = await response.body()
                            # Compress content with zstd
                            content_compressed = compress(content)
                            incidental["content_compressed"] = (
                                content_compressed
                            )
                            incidental["content_size_original"] = len(content)
                            incidental["content_size_compressed"] = len(
                                content_compressed
                            )
                        except Exception as e:
                            logger.debug(
                                f"Failed to capture content for {request.url}: {e}"
                            )
                    break

        page.on("request", on_request)
        page.on("response", on_response)

    async def close(self) -> None:
        """Close driver and cleanup resources."""
        # Close page if open
        if self._page:
            await self._page.close()
            self._page = None

        # Call parent close to persist state
        await super().close()
