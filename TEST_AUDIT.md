# Test Audit: Driver & BaseScraper Features

Cross-reference of features against existing test coverage.

**Legend:** Tested | Partial | **UNTESTED**

---

## 1. SyncDriver (`kent/driver/sync_driver.py`)

### Lifecycle & Core Loop

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `run()` seeds queue with entry requests | Tested | `test_02` integration, `test_07`, `test_14` |
| `run()` processes queue until empty | Tested | `test_02` integration |
| `run()` fires `on_run_start` callback | Tested | `test_14_lifecycle_hooks.py` |
| `run()` fires `on_run_complete` with status/error | Tested | `test_14_lifecycle_hooks.py` |
| `run()` discovers and seeds speculative requests | Partial | `test_speculate_decorator.py` (decorator only) |
| Graceful shutdown via `stop_event` | Tested | `test_stop_event.py` |

### Request Processing

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `enqueue_request()` resolves URL from context | Tested | `test_02`, `test_03` |
| `enqueue_request()` checks `duplicate_check` | Tested | `test_16_deduplication.py` |
| `enqueue_request()` uses priority queue (heapq) | Tested | `test_15_priority_queue.py` |
| FIFO ordering within same priority | Tested | `test_15_priority_queue.py` |
| `resolve_request()` via `request_manager` | Tested | `test_02` integration |
| `resolve_archive_request()` downloads file | Tested | `test_04_archive_request.py` |
| `resolve_archive_request()` calls `on_archive` | Tested | `test_13_archive_callback.py` |

### Generator Processing (`_process_generator`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Handles `ParsedData` yield | Tested | `test_02`, `test_07` |
| Handles `Request` yield (navigating) | Tested | `test_02_navigating_request.py` |
| Handles `Request` yield (nonnavigating) | Tested | `test_03_nonnavigating_request.py` |
| Handles `Request` yield (archive) | Tested | `test_04_archive_request.py` |
| Catches `ScraperAssumptionException` | Tested | `test_08_structural_errors.py` |
| Calls `on_structural_error` callback | Tested | `test_08_structural_errors.py` |
| Continue/halt based on callback return | Tested | `test_08_structural_errors.py` |

### Data Handling

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `handle_data()` validates deferred data | Tested | `test_09_data_validation.py` |
| `handle_data()` calls `on_data` callback | Tested | `test_07_callbacks.py` |
| `handle_data()` calls `on_invalid_data` on failure | Tested | `test_09_data_validation.py` |
| Re-raises if no callbacks provided | Tested | `test_09_data_validation.py` |

### Speculation

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `_discover_speculate_functions()` | Partial | `test_speculate_decorator.py` (decorator metadata) |
| `_seed_speculative_queue()` with `definite_range` | **UNTESTED** | — |
| `_seed_speculative_queue()` with default range | **UNTESTED** | — |
| `_extend_speculation()` near ceiling | **UNTESTED** | — |
| `_track_speculation_outcome()` success/failure | **UNTESTED** | — |
| `fails_successfully()` soft 404 detection | **UNTESTED** | — |
| Stop when `consecutive_failures >= plus` | **UNTESTED** | — |

### Callbacks

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `on_data` | Tested | `test_07_callbacks.py` |
| `on_structural_error` | Tested | `test_08_structural_errors.py` |
| `on_invalid_data` | Tested | `test_09_data_validation.py` |
| `on_transient_exception` | Tested | `test_10_transient_exceptions.py` |
| `on_archive` | Tested | `test_13_archive_callback.py` |
| `on_run_start` | Tested | `test_14_lifecycle_hooks.py` |
| `on_run_complete` | Tested | `test_14_lifecycle_hooks.py` |
| `duplicate_check` | Tested | `test_16_deduplication.py` |

---

## 2. AsyncDriver (`kent/driver/async_driver.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `run()` with single worker | Tested | `test_async_driver.py` |
| `run()` with multiple workers | Tested | `test_async_driver.py` |
| `asyncio.PriorityQueue` ordering | Tested | `test_async_driver.py` |
| Graceful shutdown via `asyncio.Event` | Tested | `test_async_driver.py` |
| `_worker()` handles transient exceptions | **UNTESTED** | — |
| `_worker()` catches `CancelledError` | **UNTESTED** | — |
| `_queue_lock` for thread-safe counter | **UNTESTED** | — |
| `_speculation_lock` for concurrent state | **UNTESTED** | — |
| Async speculation seed/extend/track | **UNTESTED** | — |
| Async `on_archive` callback | **UNTESTED** | — |
| Async `on_structural_error` callback | **UNTESTED** | — |
| Async `on_invalid_data` callback | **UNTESTED** | — |
| Async `on_transient_exception` callback | **UNTESTED** | — |
| Cleanup of owned `request_manager` | **UNTESTED** | — |

---

## 3. PlaywrightDriver (`kent/driver/playwright_driver/playwright_driver.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `open()` classmethod lifecycle | Tested | `test_playwright_driver_integration.py` |
| Browser context creation | Tested | `test_playwright_driver_integration.py` |
| DB persistence of browser config | Tested | `test_playwright_db_persistence.py` |
| DOM snapshot model (render + serialize) | Tested | `test_playwright_driver_integration.py` |
| `_execute_via_navigation()` ViaFormSubmit | Partial | `test_via_field.py` (dataclass only) |
| `_execute_via_navigation()` ViaLink | Partial | `test_via_field.py` (dataclass only) |
| `_process_await_list()` WaitForSelector | Tested | `test_wait_conditions.py` |
| `_process_await_list()` WaitForLoadState | Tested | `test_wait_conditions.py` |
| `_process_await_list()` WaitForURL | Tested | `test_wait_conditions.py` |
| `_process_await_list()` WaitForTimeout | Tested | `test_wait_conditions.py` |
| Autowait retry on structural errors | Partial | `test_step_decorator_playwright_features.py` |
| `_is_playwright_compatible_selector()` | **UNTESTED** | — |
| `_compose_absolute_selector()` | **UNTESTED** | — |
| `_register_network_listeners()` incidental tracking | **UNTESTED** | — |
| Rate limiter integration | **UNTESTED** | — |
| `excluded_resource_types` filtering | **UNTESTED** | — |
| Page reuse for sequential navigations | **UNTESTED** | — |
| `close()` cleanup | **UNTESTED** | — |

---

## 4. Callback Utilities (`kent/driver/callbacks.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `save_to_jsonl_file()` | Tested | `test_07_callbacks.py` |
| `save_to_jsonl_path()` | Tested | `test_07_callbacks.py` |
| `print_data()` | **UNTESTED** | — |
| `count_data()` | Tested | `test_07_callbacks.py` |
| `combine_callbacks()` | Tested | `test_07_callbacks.py` |
| `validate_data()` | **UNTESTED** | — |
| `log_and_validate_invalid_data()` | Tested | `test_09_data_validation.py` |
| `default_archive_callback()` | Tested | `test_13_archive_callback.py` |

---

## 5. BaseScraper (`kent/data_types.py`)

### Class Attributes (Metadata)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `court_ids` | **UNTESTED** | — |
| `court_url` | **UNTESTED** | — |
| `data_types` | **UNTESTED** | — |
| `status` (ScraperStatus enum) | **UNTESTED** | — |
| `version` | **UNTESTED** | — |
| `last_verified` | **UNTESTED** | — |
| `oldest_record` | **UNTESTED** | — |
| `requires_auth` | **UNTESTED** | — |
| `rate_limits` | **UNTESTED** | — |
| `ssl_context` | **UNTESTED** | — |

### Instance Methods

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `__init__(params)` | Tested | `test_entry_decorator.py` |
| `get_params()` | **UNTESTED** | — |
| `get_entry()` default generator | Tested | `test_02` (via driver) |
| `get_continuation(name)` resolves method | Tested | `test_02` (via driver) |
| `get_ssl_context()` | **UNTESTED** | — |
| `fails_successfully(response)` | **UNTESTED** | — |
| `initial_seed(params)` dispatch | Tested | `test_entry_decorator.py` |

### Class Methods (Introspection)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `list_steps()` | **UNTESTED** | — |
| `list_speculators()` | **UNTESTED** | — |
| `list_entries()` | Tested | `test_entry_decorator.py` |
| `schema()` JSON Schema generation | Tested | `test_entry_decorator.py` |

---

## 6. Request Types (`kent/data_types.py`)

### BaseRequest

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Deep copy of `accumulated_data` | Tested | `test_05_accumulated_data.py` |
| Deep copy of `aux_data` | Tested | `test_06_aux_data.py` |
| Deep copy of `permanent` | Tested | `test_18_permanent_data.py` |
| `resolve_url()` absolute URLs | Tested | `test_02_navigating_request.py` |
| `resolve_url()` relative URLs | Tested | `test_02_navigating_request.py` |
| `deduplication_key` generation (SHA256) | Tested | `test_16_deduplication.py` |
| `SkipDeduplicationCheck` marker | Tested | `test_16_deduplication.py` |
| Custom `deduplication_key` | Tested | `test_16_deduplication.py` |
| `_merge_permanent_into_request()` | Tested | `test_18_permanent_data.py` |

### Request

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Updates `current_location` on resolve (navigating) | Tested | `test_03_nonnavigating_request.py` |
| Preserves `current_location` (nonnavigating) | Tested | `test_03_nonnavigating_request.py` |
| `resolve_from()` | Tested | `test_02`, `test_03`, `test_04` |
| `speculative()` creates speculative copy | Partial | `test_speculate_decorator.py` |
| `expected_type` field (archive) | Tested | `test_04_archive_request.py` |
| Default priority=1 for archive | Tested | `test_15_priority_queue.py` |

### Response / ArchiveResponse

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `status_code`, `headers`, `content`, `text` | Tested | `test_02_navigating_request.py` |
| `url` (final after redirects) | Tested | `test_02_navigating_request.py` |
| `request` back-reference | Tested | `test_02_navigating_request.py` |
| `ArchiveResponse.file_url` | Tested | `test_04_archive_request.py` |

### ParsedData

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `data` wrapping | Tested | `test_02_navigating_request.py` |
| `unwrap()` | Tested | `test_02_navigating_request.py` |
| Frozen dataclass | Tested | `test_02_navigating_request.py` |

---

## 7. Decorators (`kent/common/decorators.py`)

### @step

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Marks methods as parsing steps | Tested | `test_19_step_decorators.py` |
| Auto-injection of `response` | Tested | `test_19_step_decorators.py` |
| Auto-injection of `request` | Tested | `test_19_step_decorators.py` |
| Auto-injection of `json_content` | Tested | `test_19_step_decorators.py` |
| Auto-injection of `lxml_tree` | Tested | `test_19_step_decorators.py` |
| Auto-injection of `page` (PageElement) | Tested | `test_19_step_decorators.py` |
| Auto-injection of `text` | Tested | `test_19_step_decorators.py` |
| Auto-injection of `local_filepath` | **UNTESTED** | — |
| Auto-injection of `accumulated_data` | **UNTESTED** | — |
| Auto-injection of `aux_data` | **UNTESTED** | — |
| Auto-injection of `previous_request` | **UNTESTED** | — |
| `priority` metadata | Tested | `test_19_step_decorators.py` |
| `encoding` metadata | **UNTESTED** | — |
| `await_list` metadata | Tested | `test_step_decorator_playwright_features.py` |
| `auto_await_timeout` metadata | Tested | `test_step_decorator_playwright_features.py` |
| Callable continuation to string resolution | Tested | `test_19_step_decorators.py` |
| `get_step_metadata()` / `is_step()` | **UNTESTED** | — |

### @speculate

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Decorator metadata storage | Tested | `test_speculate_decorator.py` |
| `observation_date` | Tested | `test_speculate_decorator.py` |
| `highest_observed` | Tested | `test_speculate_decorator.py` |
| `largest_observed_gap` | Tested | `test_speculate_decorator.py` |
| Signature validation (one ID param) | **UNTESTED** | — |

### @entry

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Marks entry point methods | Tested | `test_entry_decorator.py` |
| `return_type` metadata | Tested | `test_entry_decorator.py` |
| `param_types` introspection | Tested | `test_entry_decorator.py` |
| `speculative` flag | Tested | `test_entry_decorator.py` |
| Parameter validation via `validate_params()` | Tested | `test_entry_decorator.py` |
| JSON Schema generation | Tested | `test_entry_decorator.py` |
| Pydantic BaseModel parameter types | Tested | `test_entry_decorator.py` |
| Primitive parameter types (str, int, date) | Tested | `test_entry_decorator.py` |

---

## 8. Page Element Interface (`kent/common/page_element.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `query_xpath()` | Tested | `test_page_element.py`, `test_lxml_page_element.py` |
| `query_xpath_strings()` | Tested | `test_lxml_page_element.py` |
| `query_css()` | Tested | `test_page_element.py`, `test_lxml_page_element.py` |
| `text_content()` | Tested | `test_lxml_page_element.py` |
| `get_attribute()` | Tested | `test_lxml_page_element.py` |
| `inner_html()` | Tested | `test_lxml_page_element.py` |
| `tag_name()` | Tested | `test_lxml_page_element.py` |
| `find_form()` | Tested | `test_page_element.py` |
| `find_links()` | Tested | `test_page_element.py` |
| `links()` | Tested | `test_page_element.py` |
| `Form.submit()` | Tested | `test_page_element.py` |
| `Form.get_field()` | Tested | `test_page_element.py` |
| `Link.follow()` | Tested | `test_page_element.py` |
| ViaLink frozen dataclass | Tested | `test_via_field.py`, `test_page_element.py` |
| ViaFormSubmit frozen dataclass | Tested | `test_via_field.py`, `test_page_element.py` |

---

## 9. CheckedHtmlElement (`kent/common/checked_html.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `checked_xpath()` with count validation | Tested | `test_08_structural_errors.py` |
| `checked_css()` with count validation | Tested | `test_08_structural_errors.py` |
| Raises `HTMLStructuralAssumptionException` | Tested | `test_08_structural_errors.py` |
| Reports to active `XPathObserver` | Tested | `test_xpath_observer.py` |
| Nested query tracking with `request_url` | Tested | `test_08_structural_errors.py` |
| text()/attribute() returning strings | Tested | `test_08_structural_errors.py` |

---

## 10. SelectorObserver (`kent/common/selector_observer.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `record_query()` | Tested | `test_selector_observer.py` |
| Nested parent-child relationships | Tested | `test_selector_observer.py` |
| Query deduplication | Tested | `test_selector_observer.py` |
| `simple_tree()` output | Tested | `test_selector_observer.py` |
| `json()` output | Tested | `test_selector_observer.py` |
| `compose_absolute_selector()` | Tested | `test_selector_observer.py` |

---

## 11. XPathObserver (`kent/common/xpath_observer.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Context manager enter/exit | Tested | `test_xpath_observer.py` |
| `record_query()` | Tested | `test_xpath_observer.py` |
| `push_context()` / `pop_context()` | Tested | `test_xpath_observer.py` |
| `get_active_observer()` | Tested | `test_xpath_observer.py` |

---

## 12. Request Manager (`kent/common/request_manager.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `SyncRequestManager.resolve_request()` | Tested | `test_02` integration (via driver) |
| `SyncRequestManager` context manager | **UNTESTED** | — |
| `SyncRequestManager` custom SSL context | **UNTESTED** | — |
| `SyncRequestManager` timeout config | **UNTESTED** | — |
| `AsyncRequestManager.resolve_request()` | Tested | `test_async_driver.py` (via driver) |
| `AsyncRequestManager` context manager | **UNTESTED** | — |
| `SQLBackedAsyncRequestManager` retry/backoff | **UNTESTED** | — |
| Exponential backoff calculation | **UNTESTED** | — |
| Max backoff time enforcement | **UNTESTED** | — |
| 5xx → `HTMLResponseAssumptionException` | Tested | `test_10_transient_exceptions.py` |
| Timeout → `RequestTimeoutException` | Tested | `test_10_transient_exceptions.py` |

---

## 13. Exceptions (`kent/common/exceptions.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `ScraperAssumptionException` | Tested | `test_08_structural_errors.py` |
| `HTMLStructuralAssumptionException` | Tested | `test_08_structural_errors.py` |
| `DataFormatAssumptionException` | Tested | `test_09_data_validation.py` |
| `TransientException` | Tested | `test_10_transient_exceptions.py` |
| `HTMLResponseAssumptionException` | Tested | `test_10_transient_exceptions.py` |
| `RequestTimeoutException` | Tested | `test_10_transient_exceptions.py` |
| `RequestFailedHalt` | **UNTESTED** | — |
| `RequestFailedSkip` | **UNTESTED** | — |

---

## 14. Deferred Validation (`kent/common/deferred_validation.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `DeferredValidation` wrapper creation | Tested | `test_09_data_validation.py` |
| `confirm()` validates and returns model | Tested | `test_09_data_validation.py` |
| `confirm()` raises `DataFormatAssumptionException` | Tested | `test_09_data_validation.py` |
| `raw_data` property | Tested | `test_09_data_validation.py` |
| `model_name` property | **UNTESTED** | — |

---

## 15. Searchable Fields (`kent/common/searchable.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Searchable field support | Tested | `test_searchable_fields.py` |
| `SpeculateFunctionConfig` | Partial | `test_speculate_decorator.py` |

---

## 16. Selector Utilities (`kent/common/selector_utils.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| `can_playwright_wait()` | Tested | `test_selector_utils.py` |

---

## 17. Consumer Data Models (`kent/common/data_models.py`)

| Feature | Tested? | Test Location |
|---------|---------|---------------|
| Model protocol compliance | Tested | `test_consumer_model_compliance.py` |

---

## Summary of Gaps

### High Priority (Core driver logic not directly tested)

1. **Speculation engine** — `_seed_speculative_queue()`, `_extend_speculation()`, `_track_speculation_outcome()`, and `fails_successfully()` have no dedicated unit tests. The @speculate decorator metadata is tested, but the driver's runtime speculation loop is not.

2. **AsyncDriver callbacks** — The async driver's handling of `on_structural_error`, `on_transient_exception`, `on_invalid_data`, and `on_archive` are not tested. Only basic multi-page and multi-worker scenarios are covered.

3. **AsyncDriver worker error paths** — `CancelledError` handling and transient exception retry within `_worker()` are untested.

4. **SQLBackedAsyncRequestManager retry/backoff** — Exponential backoff logic and max backoff enforcement have no tests.

### Medium Priority (Playwright-specific gaps)

5. **PlaywrightDriver network listener** — `_register_network_listeners()` for incidental request tracking is untested.

6. **PlaywrightDriver selector composition** — `_compose_absolute_selector()` and `_is_playwright_compatible_selector()` are untested at the driver level (though SelectorObserver's `compose_absolute_selector` is tested).

7. **PlaywrightDriver rate limiter** — No test verifies rate limiting pacing.

8. **PlaywrightDriver page reuse** — Sequential navigation page reuse behavior untested.

### Lower Priority (Utility and metadata gaps)

9. **`print_data()` and `validate_data()` callbacks** — Two callback factory functions have no tests.

10. **BaseScraper class attributes** — Metadata fields like `court_ids`, `status`, `version`, etc. are never validated by tests.

11. **`RequestFailedHalt` / `RequestFailedSkip`** — Exception types exist but have no test exercising the driver's handling of them.

12. **@step injection of `local_filepath`, `accumulated_data`, `aux_data`, `previous_request`** — These injection paths through the decorator are untested (the underlying data flow is tested elsewhere, but not via decorator injection).

13. **`list_steps()` and `list_speculators()` introspection** — BaseScraper class methods with no tests.

14. **Request manager context managers and SSL** — `SyncRequestManager`/`AsyncRequestManager` as context managers and custom SSL are untested directly.
