# DevDriver Debugging Wishlist

This document catalogs information that would accelerate debugging scraper issues.

## High-Value Questions

### 1. "What did the parser yield?"
When a step completes but doesn't produce expected results, I need to know:
- Did the generator yield anything?
- What types of yields occurred (NavigatingRequest, ParsedData, etc.)?
- If yields occurred, what were their key attributes?

**Proposed**: Add a `yields` table tracking all generator yields per request.

### 2. "Why did parsing stop early?"
When a parser returns early (via `return` or exception), I need to know:
- At what line did the return occur?
- What condition triggered the early return?
- What were the local variable values at that point?

**Proposed**: Add optional "parsing trace" mode that logs decision points.

### 3. "What XPath/regex matches occurred?"
When parsing HTML, I need to see:
- What XPath queries were run?
- How many elements matched?
- Sample of matched element content (first few chars)

**Proposed**: Wrap XPath calls in instrumented helpers that log to a `parsing_trace` table.

### 4. "Show me the actual HTML structure"
When I suspect the HTML structure changed:
- Pretty-printed snippet around expected elements
- Tag hierarchy leading to target elements
- Class/id attributes at each level

**Proposed**: Response viewer that shows HTML tree structure, not just raw content.

## Current Debug Session

### Issue: Connecticut oral arguments scraper
- 9 requests completed, 0 results
- POST requests working (varied response sizes: 99KB-244KB)
- `parse_court_year_page` completing but not yielding NavigatingRequests

### What I need to know right now:
1. Did `_process_oral_args_entries` find any sections?
2. Did `_parse_oral_arg_section` get called?
3. If called, at what point did each invocation return early?
4. What were the XPath match counts for each query?

### Root cause found (2025-01-15):
The XPath for finding content sections in the "old format" path was wrong:
- **Expected**: `following-sibling::article[1]//section`
- **Actual structure**:
  ```html
  <article class="collapsable">
    <button>First Term:</button>
    <div class="collapsable_cont">     <!-- sibling is div, not article! -->
      <article class="ResponseCaseList">
        <section class="fullWidth">...
  ```
- **Fix**: `following-sibling::div[1]//article[@class='ResponseCaseList']//section[@class='fullWidth']`

### Debugging lesson:
If I had a way to see "XPath query X matched 0 elements" during the run, this
would have been immediately obvious. The fix took:
1. Query database for response content
2. Decompress with zstandard
3. Parse with lxml
4. Test XPath manually
5. Inspect actual HTML structure

A "parsing trace" feature would have shown this in the web UI immediately.

## Proposed Feature: Parsing Trace

### What it would capture:
For each step execution:
1. **XPath queries**: query string, match count, first match sample
2. **Regex matches**: pattern, input sample, match/no-match, groups
3. **Generator yields**: type, key attributes (url, docket_id, etc.)
4. **Early returns**: line number, reason (condition failed, no matches, etc.)

### Implementation options:
1. **Decorator-based**: Wrap step methods to capture yields
2. **Context manager**: `with parsing_trace(response_id):` that instruments xpath/regex
3. **Logging-based**: Structured logging that gets stored in DB

### Storage:
```sql
CREATE TABLE parsing_traces (
    id INTEGER PRIMARY KEY,
    response_id INTEGER REFERENCES responses(id),
    trace_type TEXT,  -- 'xpath', 'regex', 'yield', 'return'
    query_or_pattern TEXT,
    match_count INTEGER,
    sample_content TEXT,
    metadata_json TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
