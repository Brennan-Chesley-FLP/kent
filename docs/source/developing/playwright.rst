=================
Playwright Driver
=================

Location: ``kent/driver/playwright_driver/``

The Playwright driver handles JavaScript-heavy websites using browser
automation. It extends ``PersistentDriver``, inheriting all its persistence
and debugging capabilities while adding browser-based request execution.

The Playwright driver is almost always **auto-selected** because the scraper
declares a ``driver_requirements`` entry (e.g., ``JS_EVAL``, ``HCAP_HANDLER``).


DOM Snapshot Model
==================

The Playwright driver maintains **step function purity** by never passing
live browser references to scraper steps. The processing flow for each
request:

1. Navigate the browser to the target page (via direct URL, link click, or form submit)
2. Process await_list conditions (wait for selectors, load states, etc.)
3. Serialize the rendered DOM to an HTML string via ``page.content()``
4. Parse the HTML with lxml and inject it as an ``LxmlPageElement``
5. Run the scraper's ``@step`` continuation against the parsed HTML

This means scraper steps receive the same ``LxmlPageElement`` interface
regardless of whether the HTTP or Playwright driver is running. Scraper
code is driver-agnostic.

The DOM snapshot is always captured, even on timeout or error, so the
response is available in the database for debugging.


Parent Request Replay
=====================

One architectural challenge for browser-based scraping is that
requests form a **tree**: a list page produces detail page requests, which
produce download requests, etc. . Parent replays are a way of handling that
tree with a finite number of browser tabs/workers.

The Playwright driver solves this with **route-intercepted parent replay**
(``_setup_tab_with_parent_response``):

.. md-mermaid::
    :class: align-center

    flowchart TB
        Start["Process request<br/>(has parent + via)"] --> Fetch["Fetch parent's stored<br/>response from DB"]
        Fetch --> Decompress["Decompress HTML content"]
        Decompress --> Route["Set up Playwright route<br/>to intercept parent URL"]
        Route --> Goto["page.goto(parent_url)<br/>→ route handler serves<br/>cached HTML"]
        Goto --> Unroute["Remove route<br/>(future navigations hit real server)"]
        Unroute --> Via["Execute via action<br/>(click link / submit form)"]
        Via --> Real["Browser navigates to<br/>real server"]
        Real --> Snapshot["Capture DOM snapshot"]

**How it works:**

1. Query the parent request's stored, compressed response from the database
2. Decompress the HTML content
3. Register a Playwright ``page.route()`` handler that intercepts the parent's
   URL and serves the cached HTML with its original headers and status code
4. Navigate to the parent's URL -- the route handler intercepts and serves
   the cached page, so no network request is made
5. Immediately unroute so all subsequent navigations hit the real server
6. Execute the ``via`` action (click link or submit form) on the cached page,
   which triggers a real browser navigation to the target URL

This is a compromise. It works enough of the time, capturing enough of the
javascript and html state to be useful. In practice, it appeared a more robust
approach than stateful tree-climbing via history navigation, and tab forking.

**Fallback:** If the parent has no stored response because it is a seeded request,
the driver falls back to a direct ``page.goto()`` with the request's URL.


Via Navigation
==============

When a scraper uses ``find_links`` or ``find_form``/``submit()``, the
resulting ``Request`` carries a ``ViaLink`` or ``ViaFormSubmit`` descriptor.
The Playwright driver uses these to replay browser actions rather than
navigating directly to URLs.

ViaLink
-------

``_execute_via_navigation`` for ``ViaLink``:

1. **Phase 1 (structural):** Wait for the link selector in the live DOM
   (``page.wait_for_selector``). If not found, raises
   ``HTMLStructuralAssumptionException``.
2. **Phase 2 (navigation):** Click the element inside
   ``page.expect_navigation()`` to wait for the resulting page load.
   Timeout errors propagate as ``TransientException`` for retry.

ViaFormSubmit
-------------

``_execute_via_navigation`` for ``ViaFormSubmit``:

1. **Phase 1 (structural):** Wait for the form selector. If not found,
   raises ``HTMLStructuralAssumptionException``.
2. **Fill fields:** Iterate ``field_data`` and fill each field by name.
   The driver handles different input types:

   - ``<select>``: ``select_option()``
   - ``<input type="radio">``: Check the matching radio in the group
   - ``<input type="checkbox">``: Set ``checked`` via JS evaluation
   - ``<input type="hidden">`` or invisible inputs: Set ``value`` via JS
     evaluation (important for ASP.NET ViewState, Telerik DatePicker, etc.)
   - Visible text inputs: ``fill()``

3. **Phase 2 (submit):** Click the submit element inside
   ``page.expect_navigation()``. Three submit strategies:

   - If ``submit_selector`` is set: click that specific element
   - If ``__EVENTTARGET`` is in ``field_data``: ASP.NET postback -- call
     ``form.submit()`` via JS to avoid the button-click event conflicting
     with the ``__EVENTTARGET`` postback
   - Otherwise: click the first ``button[type="submit"]`` or
     ``input[type="submit"]``

Via Downloads
-------------

Archive requests with ``via`` descriptors use ``_execute_via_download``,
which is identical to the navigation flow except it wraps the click in
``page.expect_download()`` instead of ``page.expect_navigation()``. The
downloaded file is saved via the archive handler.


Interstitial Handling
=====================

Location: ``kent/driver/interstitials.py``

Interstitial handlers deal with CAPTCHA challenges, bot-detection pages, and
other barriers that appear between navigation and the target content. They
run on the **live Playwright page** after navigation but before the DOM
snapshot.

The Racing Pattern
------------------

When interstitial handlers are configured, the driver **races** the scraper's
``await_list`` against each handler's ``waitlist()``:

.. md-mermaid::
    :class: align-center

    flowchart TB
        Nav["Page navigated"] --> Race["asyncio.wait FIRST_COMPLETED"]
        Race --> ScraperWins{"Scraper await_list<br/>completed first?"}
        ScraperWins -->|Yes| Snapshot["Proceed to<br/>DOM snapshot"]
        ScraperWins -->|No| Handler["Interstitial handler<br/>wins"]
        Handler --> Navigate["handler.navigate_through(page)<br/>(solve captcha, click through)"]
        Navigate --> Reprocess["Re-process scraper's<br/>await_list on new page"]
        Reprocess --> Snapshot

``_race_await_lists`` creates concurrent ``asyncio.Task`` for each group of
wait conditions (one for the scraper's await_list, one per interstitial
handler's waitlist). The first group to fully complete wins; all losing tasks
are cancelled.

If the scraper's conditions match first, no interstitial was present -- proceed
normally. If a handler's conditions match first, the handler gets to interact
with the live page (e.g., solve the CAPTCHA), then the scraper's conditions
are re-checked on the resulting page.

InterstitialHandler Protocol
----------------------------

Each handler implements two methods:

``waitlist() -> list[WaitCondition]``
    Conditions that indicate this interstitial is present. All must match
    (conjunction). For example, hCaptcha returns
    ``[WaitForSelector("div.h-captcha")]``.

``navigate_through(page: Page) -> None``
    Interact with the live page to get past the interstitial.

Built-in Handlers
-----------------

``HCaptchaHandler``
    Clicks ``div.h-captcha``. In headless Firefox with ``navigator.webdriver``
    overridden, hCaptcha auto-solves; the JS callback submits the form.

``ReCaptchaHandler``
    Multi-step audio challenge solver:

    1. Reveal hidden parent elements of ``.g-recaptcha``
    2. Click the reCAPTCHA checkbox in the anchor iframe
    3. Race: check for auto-solve (checkmark) vs. audio challenge (bframe)
    4. If challenge: click audio button, intercept the audio payload response
    5. Transcribe audio via the ``AudioTranscriber`` protocol (default:
       ``LocalStenoTranscriber`` posting to a local server)
    6. Fill and submit the answer
    7. Verify the checkmark appears

The ``INTERSTITIAL_HANDLERS`` dict maps ``DriverRequirement`` values to
handler instances:

.. code-block:: python

    INTERSTITIAL_HANDLERS = {
        DriverRequirement.HCAP_HANDLER: HCaptchaHandler(),
        DriverRequirement.RCAP_HANDLER: ReCaptchaHandler(LocalStenoTranscriber()),
    }


Autowait
========

The ``auto_await_timeout`` parameter on ``@step`` enables automatic retry
when content loads progressively. The flow in
``_process_generator_with_autowait``:

1. Run the step's continuation against the current DOM snapshot
2. If an ``HTMLStructuralAssumptionException`` is raised:

   a. Check if the timeout budget is exhausted -- if so, re-raise
   b. Check if the failing selector is Playwright-compatible (rejects
      ``text()`` nodes, EXSLT extensions, XPath variables)
   c. Use the ``SelectorObserver`` from the step's metadata to compose an
      **absolute selector** from the relative one that failed (walking the
      parent chain)
   d. Wait for that absolute selector in the live browser DOM
   e. Re-snapshot the DOM via ``page.content()``
   f. Update the stored response in the database
   g. Retry the step with the fresh snapshot

3. Repeat until the step succeeds or the timeout expires


Browser Lifecycle
=================

Worker Pages
------------

Each concurrent worker gets a long-lived ``WorkerPage`` wrapping a Playwright
``Page``. ``WorkerPage`` encapsulates per-request state (incidental network
requests) so concurrent workers don't corrupt each other's data.

Between requests, ``reset_for_reuse()`` navigates to ``about:blank`` and
clears the incidental request list, providing a clean slate without the
overhead of closing and reopening the page.

Browser Crash Recovery
----------------------

If the browser process crashes (connection lost), the driver detects it via
``_is_connection_dead()`` and, under a ``_browser_restart_lock``, restarts
the browser and context. All worker pages are discarded (they reference
the dead browser), and workers acquire fresh pages on their next iteration.

This is only supported for the standard (non-persistent) launch path.
Persistent contexts cannot be safely restarted.

Browser Profiles
----------------

``BrowserProfile`` (``browser_profile.py``) configures the browser:

- Browser type (Chromium, Firefox, WebKit) and channel
- Launch options (headless, args)
- Context options (viewport, user agent, locale, timezone)
- Protocol parameters (CDP port, assistant mode)
- Init scripts (injected into every page)
- Persistent context flag (for Cloudflare bypass, session persistence)

The scraper's ``driver_requirements`` determine which profile is used:

- ``DriverRequirement.FF_ALIKE``: Firefox profile auto-resolved from
  ``$KENT_HOME/profiles/firefox/``
- ``DriverRequirement.CHROME_ALIKE``: Chromium profile from
  ``$KENT_HOME/profiles/chrome/``
- Default: Chromium with standard options

On resume, the browser configuration is reloaded from the ``run_metadata``
table so the same profile is used across restarts.

Incidental Request Tracking
---------------------------

``WorkerPage`` registers ``page.on("request")`` and ``page.on("response")``
listeners that capture all network activity during page rendering. Each
incidental request records:

- Resource type, method, URL, headers
- Response status, headers, compressed content
- Timing (started_at_ns, completed_at_ns)
- Cache status (from service worker)

These are stored in the ``incidental_requests`` table (FK to the parent
request) after each DOM snapshot. Resource types like images, media, and
fonts are excluded from content capture by default to save space.
