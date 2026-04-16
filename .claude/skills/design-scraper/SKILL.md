---
name: design-scraper
description: Design and implement a kent scraper for an appellate court website. Invoked with a URL to explore. Uses Playwright for site reconnaissance, then produces DESIGN.md, models.py, and scraper.py.
user-invocable: true
argument-hint: <url>
---

# Design Scraper

You are designing and implementing a kent framework scraper for an appellate
court website. The user provides a URL as the argument to this skill.

See [kent-api-reference.md](kent-api-reference.md) for the kent framework API.

## Output Files

Locate the juriscraper scrapers directory:

```bash
find . -path "*/juriscraper/sd/state" -type d 2>/dev/null | head -1
```

All files go under that directory at `{state}/{domain_underscored}/`:

- `DESIGN.md` — Site analysis and design decisions
- `models.py` — Pydantic data models (ScrapedData subclasses)
- `scraper.py` — Scraper implementation (BaseScraper subclass)
- `__init__.py` — Empty package init

Derive `{state}` from the court's US state (lowercase, underscores:
`california`, `new_york`). Derive `{domain_underscored}` from the hostname
with dots replaced by underscores (e.g., `appellatecases.courtinfo.ca.gov` →
`appellatecases_courtinfo_ca_gov`).

If the target directory already exists, read existing files before overwriting.

---

## Phase 1: Site Reconnaissance

1. **Navigate** to the URL with `browser_navigate`.
2. **Snapshot** the page (`browser_snapshot`) to see forms, links, layout.
3. **Identify all search forms** — note each form's action URL, method
   (GET/POST), and every field (name, type, required, options for selects).
4. **Identify all courts covered** — look for:
   - Court selector dropdowns or radio buttons
   - URL parameters (e.g., `dist=3`, `court=SC`)
   - Separate pages per court
   Record every court's internal identifier, display name, and any division
   info.
5. **Check for a calendar / oral arguments section** — often has date-based
   search even when the main case search doesn't. If found, note the URL and
   search fields.

---

## Phase 2: Probe Search Interfaces

Rank the available search modes by scraping utility:

| Rank | Mode | Why |
|------|------|-----|
| 1 | Date-based search | Enables incremental scraping by date range |
| 2 | Case number search | Enables speculative sequential enumeration |
| 3 | Party name search | Useful for probing docket number formats |
| 4 | Attorney search | Rarely useful for bulk scraping |

### If date search exists — test it
- Submit a 7-day window for a recent period.
- Note date field format (`mm/dd/yyyy`, ISO, etc.).
- Check result count caps (some APIs cap at 10,000).
- Check pagination (GET params, POST body, JS-driven).

### If case number search exists — test it
- Try a known number (user may provide one, or find one via party search).
- Note whether it redirects directly to the case or shows a results list.
- Note any **bot protection fields** — hidden inputs auto-set by JavaScript.
  These must be included in POST requests when using httpx.

### Party name probing (always do this)
Search for **"smith"** to discover:
- **Docket number format** per court — prefix, sequential digits, year
  component. Examples: `C000125` (letter + digits), `SC-2023-0123`
  (court-year-seq), `2024-00003` (year-seq).
- **Result count** and pagination behavior.
- Whether trial court numbers appear alongside appellate numbers.

Search in multiple courts if the site covers more than one, since different
courts often use different docket number prefixes.

### Speculative entry assessment
If there is no date-based search, speculative entry is required. Determine:
- The docket number pattern per court (prefix + sequential number).
- The approximate range (lowest and highest numbers observed).
- The largest gaps between sequential numbers.
- Whether numbers reset yearly or are continuous.
- Example: District 3 uses prefix `C` + up to 6 digits, highest observed
  `C105926` → `SimpleSpeculation(highest_observed=105926,
  largest_observed_gap=20)`.

For year-partitioned numbers (e.g., `2024-00003`), use `YearlySpeculation`
with a `YearPartition` per year.

---

## Phase 3: Explore Case Details

Click into a case result and visit **every** available tab or section. For
each tab:

1. Note the **URL pattern** and what parameters are needed (session tokens,
   doc IDs, etc.).
2. Record **every data field** displayed.
3. Check for **downloadable documents** (PDFs, audio, images).
4. Note whether content is server-rendered or loaded via JavaScript/AJAX.

### Standard tabs to look for

| Tab | Key fields |
|-----|-----------|
| Case Summary | Case type, filing date, completion date, caption, division |
| Docket / Register of Actions | Date, description, notes per entry |
| Briefs | Brief type, due date, filed date, party/attorney |
| Disposition | Outcome, date, publication status, author, citation |
| Parties & Attorneys | Names, roles, firms, addresses, phone numbers |
| Trial Court | Court name, county, case number, judge, judgment date |
| Scheduled Actions | Future events, hearing dates |
| Documents | Download links with types, dates, descriptions |

### Email notifications
Look for "subscribe to email notifications" or similar links on case pages.
If found:
- Note the URL pattern.
- Record all available notification event types (e.g., "Brief Filed",
  "Disposition", "Opinion Available Online").
- Document this in DESIGN.md.

---

## Phase 4: Technical Assessment

### HTTP vs Playwright

Test whether `httpx`/`curl` can handle the site:

```bash
curl -s -o /dev/null -w "%{http_code}" "URL"
curl -s "URL" | head -50
```

- **CloudFlare challenge page** or empty body → **Playwright required**
- **Full server-rendered HTML** → httpx works
- **JavaScript SPA** (React/Vue/Angular with empty initial HTML) → Playwright
  required

The key driver of Playwright requirement is **bot protection** (CloudFlare,
Akamai, etc.), not the server framework. ASP.NET, ColdFusion, PHP sites all
work fine with httpx when there is no JS challenge gate.

### Court ID mapping

Look up each court in CourtListener's database. Find `courts.json` by
searching for it:

```bash
find ../.. -path "*/courts_db/data/courts.json" 2>/dev/null | head -1
```

Each entry has:
- `id` — CourtListener court ID (e.g., `calctapp3d`)
- `name` — Full court name
- `type` — `appellate`, `trial`, etc.
- `level` — `colr` (court of last resort), `iac` (intermediate appellate)
- `parent` — Parent court ID for sub-courts

Search by state name and court name. Build a mapping: **site internal ID →
display name → CourtListener court ID**.

For courts with divisions that map to a single CourtListener ID
(e.g., CA District 4 Divisions 1-3 all map to `calctapp4d`), note this in
the mapping.

---

## Phase 5: Write DESIGN.md

```markdown
# {Site Name} Scraper Design

## Site Overview
- **Base URL**: {url}
- **Requires Playwright**: {Yes — CloudFlare / No — server-rendered HTML}

## Courts Covered

| Site ID | Display Name | CourtListener ID |
|---------|-------------|-----------------|
| ... | ... | ... |

## Search Capabilities
{Ranked list of search modes with notes on each}
**Recommended approach**: {date-based / speculative / hybrid}

## Docket Number Formats
{Per court: prefix pattern, sequential component, year component, examples}

## Data Available

### Case Summary
{List every field with its type}

### Docket Entries
{fields}

### Briefs
{fields}

### Disposition
{fields}

### Parties & Attorneys
{fields}

### Trial Court
{fields}

### Documents
{fields}

## Email Notifications
{Available / Not available}
{If available: URL pattern, event types, registration fields}

## Oral Arguments Calendar
{Available / Not available}
{If available: search modes, fields}

## Bot Protection Notes
{Hidden fields, session tokens, cookie requirements, redirect behavior}

## Scraper Architecture

### Entry Points
{List each @entry function with its type, params, and purpose}

### Step Functions
{Flow: entry → step1 → step2 → ... → ParsedData}

### Models
{List of ScrapedData models to create}
```

---

## Phase 6: Write models.py

Import `ScrapedData` from `kent.common.data_models`. Follow these conventions:

- Every model extends `ScrapedData`.
- Use type hints: `str`, `date`, `int`, `list[X]`, `X | None`.
- Add a docstring on every field.
- Default optional fields to `None`; default lists to `[]`.
- Prefer `str | None = None` over `Optional[str]`.
- Use `date` (not `datetime`) for date fields.
- Include a `COURT_IDS` dict mapping CourtListener IDs to display names.
- Include any site-specific config (API endpoints, court internal IDs, etc.).

### Standard model hierarchy for docket scrapers

```python
class {Prefix}DocketEntry(ScrapedData):
    """A single entry from the Register of Actions / Docket tab."""
    date_filed: date | None = None
    description: str
    notes: str | None = None

class {Prefix}Party(ScrapedData):
    """A party in the case."""
    name: str
    role: str  # e.g., "Plaintiff and Appellant"
    attorneys: list[{Prefix}Attorney] = []

class {Prefix}Attorney(ScrapedData):
    """Attorney representation record."""
    name: str
    firm: str | None = None
    address: str | None = None
    phone: str | None = None

class {Prefix}Document(ScrapedData):
    """A downloadable document from the case."""
    download_url: str
    document_type: str
    date_filed: date | None = None
    description: str | None = None
    local_path: str | None = None

class {Prefix}Docket(ScrapedData):
    """Main output — a complete appellate case docket."""
    # Searchable fields
    docket_id: str
    court_id: str
    date_filed: date | None = None
    case_name: str
    # Case metadata
    case_type: str | None = None
    ...
    # Nested data
    entries: list[{Prefix}DocketEntry] = []
    parties: list[{Prefix}Party] = []
    documents: list[{Prefix}Document] = []
    source_url: str | None = None
```

Add more models as the site warrants:
- `{Prefix}Brief` — if briefs tab has structured columns beyond docket entries
- `{Prefix}Disposition` — if disposition has multiple structured fields
- `{Prefix}TrialCourtInfo` — embedded in the main Docket rather than separate
- `{Prefix}OralArgument` — if oral arguments are a separate data type with
  their own entry point

---

## Phase 7: Write scraper.py

### Imports

```python
from __future__ import annotations

import re
from datetime import date, timedelta
from typing import TYPE_CHECKING, ClassVar
from urllib.parse import urljoin

from kent.common.decorators import entry, step
from kent.common.exceptions import TransientException
from kent.common.page_element import PageElement
from kent.common.param_models import DateRange, SpeculativeRange
from kent.common.speculation_types import SimpleSpeculation  # or YearlySpeculation
from kent.data_types import (
    BaseScraper,
    DriverRequirement,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperStatus,
    SkipDeduplicationCheck,
)
from pyrate_limiter import Duration, Rate

from .models import ...  # Import your models

if TYPE_CHECKING:
    from collections.abc import Generator
    from kent.data_types import ScraperYield
```

### Class metadata

```python
class {Name}Scraper(BaseScraper[{MainType}]):
    """Scraper for {Court Name(s)}.

    {Brief description of what's scraped and how.}
    """
    court_ids: ClassVar[set[str]] = {"id1", "id2", ...}
    court_url: ClassVar[str] = "https://..."
    data_types: ClassVar[set[str]] = {"dockets"}  # or {"dockets", "oral_arguments"}
    status: ClassVar[ScraperStatus] = ScraperStatus.IN_DEVELOPMENT
    version: ClassVar[str] = "{YYYY-MM-DD}"
    requires_auth: ClassVar[bool] = False
    rate_limits: ClassVar[list[Rate] | None] = [Rate(1, Duration.SECOND)]
    # Only if Playwright is needed (bot protection, JS SPA):
    # driver_requirements: ClassVar[list[DriverRequirement]] = [
    #     DriverRequirement.JS_EVAL, DriverRequirement.FF_ALIKE,
    # ]
```

If the scraper yields multiple top-level types, the generic parameter should
be their union: `BaseScraper[Docket | OralArgument]`.

### Entry point strategy

**Date-based search available:**
```python
@entry({Docket})
def get_dockets(self) -> Generator[Request, None, None]:
    """Fetch dockets using date range from scraper params."""
    date_gte, date_lte = self._get_date_params()
    yield Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=SEARCH_URL),
        continuation=self.parse_search_page,
        accumulated_data={"date_gte": ..., "date_lte": ...},
    )

@entry({Docket})
def get_dockets_by_date(self, date_range: DateRange) -> Generator[Request, None, None]:
    """Fetch dockets for an explicit date range."""
    ...
```

**Speculative entry (one per court):**
```python
@entry({Docket}, speculative=SimpleSpeculation(
    highest_observed=N,
    largest_observed_gap=G,
))
def fetch_{court_prefix}_docket(self, case_number: int) -> Request:
    """Speculative docket fetcher for {Court Name}."""
    docket_id = f"{PREFIX}{case_number:06d}"  # Format to match site pattern
    return Request(
        request=HTTPRequestParams(
            method=HttpMethod.POST,
            url=SEARCH_URL,
            data={"query_caseNumber": docket_id, ...},
        ),
        continuation=self.parse_search_results,
        accumulated_data={"court_id": "...", "docket_id": docket_id},
    )
```

Alternative: use `SpeculativeRange` as the parameter type (provides `.number`
and `.gap`):
```python
@entry({Docket})
def fetch_{court_prefix}_docket(self, rid: SpeculativeRange) -> Request:
    docket_id = f"{PREFIX}{rid.number:06d}"
    return self._make_search_request(docket_id, court_id="...")
```

For year-partitioned numbers use `YearlySpeculation` with `YearPartition`
entries per year.

**Oral arguments (if discovered):**
```python
@entry({OralArgument})
def get_oral_arguments_by_date(self, date_range: DateRange) -> Generator[Request, None, None]:
    """Fetch oral arguments for a date range."""
    ...
```

### Step functions

Each step function:
- Accepts injected parameters by name: `page` (PageElement), `response`
  (Response), `accumulated_data` (dict), `json_content` (dict/list),
  `text` (str), `local_filepath` (str | None)
- Uses `page.query_xpath()`, `page.find_form()`, `page.find_links()` for
  HTML parsing.
- Yields `Request` for follow-on pages and `ParsedData` for final output.
- Passes context forward via `accumulated_data`.
- Values in `accumulated_data` must be JSON-serializable. Use
  `.model_dump(mode="json")` for Pydantic models, `.isoformat()` for dates.

**Typical flow for a case detail scraper:**
```
entry (search) → parse_search_results → parse_case_summary
                                       → parse_docket_entries
                                       → parse_parties
                                       → parse_disposition
                                       → parse_trial_court
                                       → assemble_docket (yields ParsedData)
```

For sites where all tabs are separate pages, chain them via accumulated_data,
collecting fields as you go:
```python
@step()
def parse_case_summary(self, page: PageElement, response: Response,
                       accumulated_data: dict) -> Generator[...]:
    # Extract case summary fields
    accumulated_data["case_name"] = ...
    accumulated_data["case_type"] = ...
    # Yield request for next tab
    yield Request(
        request=HTTPRequestParams(method=HttpMethod.GET, url=docket_tab_url),
        continuation=self.parse_docket_entries,
        accumulated_data=accumulated_data,
    )
```

For the final step, assemble and yield the complete model:
```python
@step()
def assemble_docket(self, accumulated_data: dict) -> Generator[...]:
    docket = {Prefix}Docket(
        docket_id=accumulated_data["docket_id"],
        court_id=accumulated_data["court_id"],
        ...
    )
    yield ParsedData(data=docket)
```

### Soft-404 detection

If the site returns HTTP 200 for invalid case numbers (common), implement:
```python
def fails_successfully(self, response: Response) -> bool:
    """Return False if this is a soft-404."""
    return "case not found" not in response.text.lower()
```

### Document downloads

For downloadable documents (opinions, briefs, etc.):
```python
yield Request(
    archive=True,
    request=HTTPRequestParams(method=HttpMethod.GET, url=pdf_url),
    continuation=self.handle_document_download,
    expected_type="pdf",
    accumulated_data={...},
)
```

### Deduplication

Use `deduplication_key` on Requests to avoid visiting the same case twice when
overlapping searches produce duplicate results:

```python
yield Request(
    request=HTTPRequestParams(method=HttpMethod.GET, url=case_url),
    continuation=self.parse_case,
    deduplication_key=docket_id,  # same docket_id won't be fetched twice
)
```

For pagination requests that must always execute, skip dedup:

```python
from kent.data_types import SkipDeduplicationCheck

yield Request(
    request=HTTPRequestParams(method=HttpMethod.GET, url=next_page_url),
    continuation=self.parse_results,
    deduplication_key=SkipDeduplicationCheck(),
)
```

### Pagination

**HTML next-link pagination**: follow "Next" links with
`page.find_links("//a[contains(text(), 'Next')]", ...)`.

**API offset pagination**: track `page` in `accumulated_data`, increment,
and yield a new Request until `current_page >= total_pages`.

**Date-range splitting**: some APIs cap results (e.g., 10,000). If a search
returns the maximum, split the date range in half and re-search each half.

All pagination requests should use
`deduplication_key=SkipDeduplicationCheck()`.

### Driver requirements

If Phase 4 determines Playwright is needed, add to the class:

```python
from kent.data_types import DriverRequirement

driver_requirements: ClassVar[list[DriverRequirement]] = [
    DriverRequirement.JS_EVAL,
    DriverRequirement.FF_ALIKE,
]
```

Values: `JS_EVAL`, `FF_ALIKE`, `CHROME_ALIKE`, `HCAP_HANDLER` (hCaptcha),
`RCAP_HANDLER` (reCAPTCHA).

Steps that need to wait for JS rendering should use `@step(await_list=[...])`:

```python
from kent.data_types import WaitForLoadState, WaitForSelector

@step(await_list=[
    WaitForLoadState("networkidle"),
    WaitForSelector("table.results"),
])
def parse_results(self, page, accumulated_data):
    ...
```

---

## Checklist Before Finishing

- [ ] DESIGN.md documents all findings from Phases 1-4
- [ ] Court mapping table is complete with CourtListener IDs
- [ ] models.py has all ScrapedData models with typed fields
- [ ] scraper.py has proper class metadata (court_ids, data_types, status, version, rate_limits)
- [ ] `driver_requirements` set if Playwright needed
- [ ] Entry points cover all courts (one per court if speculative)
- [ ] Entry points cover oral arguments if the site has a calendar
- [ ] Step functions parse every tab/section discovered in Phase 3
- [ ] Pagination handled with `SkipDeduplicationCheck()` on next-page requests
- [ ] Custom `deduplication_key` set where overlapping searches may yield duplicates
- [ ] Document downloads use `archive=True`
- [ ] `accumulated_data` values are JSON-serializable
- [ ] Email notification capability is documented (not necessarily implemented)
- [ ] Bot protection fields are handled in form submissions
- [ ] `__init__.py` exists in the target directory