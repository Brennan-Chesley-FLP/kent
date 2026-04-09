# Test Organization

Living index of organized test files. Updated as files are moved out of `tests/unorganized/`.

---

## `tests/data_types/`

### `test_navigating_request.py`
- `test_navigating_request_stores_url` — Request stores the target URL
- `test_navigating_request_stores_continuation` — Request stores the continuation method name
- `test_navigating_request_defaults_to_get` — Request defaults to GET method
- `test_navigating_request_supports_post` — Request supports POST method
- `test_resolve_url_with_absolute_url` — Absolute URLs pass through resolution unchanged
- `test_resolve_url_with_relative_url` — Relative URLs resolve against current_location
- `test_resolve_url_with_relative_path` — Relative paths resolve against current_location
- `test_response_stores_status_code` — Response stores HTTP status code
- `test_response_stores_headers` — Response stores HTTP headers
- `test_response_stores_content_and_text` — Response stores content bytes and decoded text
- `test_response_stores_final_url` — Response stores the final URL after redirects
- `test_response_stores_original_request` — Response holds a reference to the originating Request
- `test_parsed_data_stores_data` — ParsedData wraps arbitrary data
- `test_parsed_data_is_frozen` — ParsedData dataclass is immutable
- `test_unwrap_returns_data` — ParsedData.unwrap() returns the inner data
- `test_parse_list_yields_navigating_requests` — Scraper list parser yields navigating Requests
- `test_parse_list_requests_have_correct_urls` — Yielded Requests have correct detail page URLs
- `test_parse_list_requests_have_correct_continuation` — Yielded Requests point to the right continuation
- `test_parse_detail_yields_parsed_data` — Scraper detail parser yields ParsedData
- `test_parse_detail_extracts_all_fields` — ParsedData contains all expected fields
- `test_driver_calls_continuation_method` — SyncDriver calls continuation methods by name
- `test_driver_returns_all_parsed_data` — SyncDriver collects all ParsedData from the pipeline
- `test_driver_returns_correct_data` — SyncDriver data matches expected values
- `test_full_scraping_pipeline` — Integration: full list->detail pipeline produces correct results
- `test_pipeline_preserves_data_integrity` — Integration: all fields survive the round-trip
- `test_continuation_as_string_is_serializable` — Continuation names are plain strings (serializable)

### `test_nonnavigating_request.py`
- `test_base_request_stores_url` — BaseRequest stores the target URL
- `test_base_request_stores_continuation` — BaseRequest stores the continuation name
- `test_base_request_defaults_to_get` — BaseRequest defaults to GET method
- `test_base_request_supports_post` — BaseRequest supports POST method
- `test_base_request_resolve_url_absolute` — BaseRequest resolves absolute URLs unchanged
- `test_base_request_resolve_url_relative` — BaseRequest resolves relative URLs against current_location
- `test_non_navigating_request_inherits_from_base` — Non-navigating Request inherits from BaseRequest
- `test_non_navigating_request_stores_url` — Non-navigating Request stores its URL
- `test_non_navigating_request_resolve_from_response` — Non-navigating Request resolves URL from Response context
- `test_navigating_request_inherits_from_base` — Navigating Request inherits from BaseRequest
- `test_navigating_request_resolve_from_response` — Navigating Request resolves URL and updates current_location
- `test_entry_request_has_no_current_location` — Entry requests start with empty current_location
- `test_navigating_request_updates_current_location` — Navigating request sets current_location to response URL
- `test_non_navigating_request_preserves_current_location` — Non-navigating request keeps parent's current_location
- `test_driver_handles_both_request_types` — SyncDriver processes navigating and non-navigating requests together
- `test_parse_list_yields_navigating_requests` — Scraper with API: list parser yields navigating requests
- `test_parse_detail_yields_non_navigating_request` — Scraper with API: detail parser yields non-navigating API request
- `test_parse_api_yields_parsed_data` — Scraper with API: API parser yields ParsedData
- `test_full_scraping_pipeline_with_api` — Integration: full pipeline with mixed request types
- `test_request_ancestry_preserved` — Request chain preserves previous_requests ancestry

---

## `tests/decorators/`

### `test_entry.py`
- `test_entry_attaches_metadata` — @entry attaches correct EntryMetadata (return_type, func_name, param_types)
- `test_entry_with_basemodel_param` — @entry records Pydantic BaseModel parameter types
- `test_speculative_entry_metadata` — @entry auto-detects Speculative protocol param and sets speculative_param
- `test_entry_metadata_is_frozen` — EntryMetadata dataclass is immutable
- `test_date_param_type` — @entry records date parameter type
- `test_complex_basemodel_param` — @entry records complex BaseModel parameter type
- `test_speculative_protocol_detected` — issubclass(PydanticModel, Speculative) works correctly
- `test_multiple_speculative_params_rejected` — @entry rejects functions with two Speculative parameters
- `test_is_entry_true` — is_entry() returns True for decorated methods
- `test_is_entry_false_for_non_entry` — is_entry() returns False for non-decorated methods
- `test_get_entry_metadata_returns_none_for_non_entry` — get_entry_metadata() returns None for non-decorated methods
- `test_discovers_all_entries` — list_entries() discovers all @entry-decorated methods
- `test_entry_info_fields` — EntryInfo has correct fields
- `test_speculative_entry_in_list` — list_entries() includes speculative entries with speculative_param
- `test_list_speculative_entries` — list_speculative_entries() filters to speculative only
- `test_validate_primitive_str` — validate_params() coerces string parameters
- `test_validate_primitive_int` — validate_params() coerces integer parameters
- `test_validate_speculative_param` — validate_params() validates Speculative models via model_validate
- `test_validate_basemodel` — validate_params() validates Pydantic BaseModel parameters
- `test_validate_date_from_string` — validate_params() parses ISO date strings
- `test_validate_date_from_date_object` — validate_params() accepts date objects
- `test_validate_missing_param_raises` — validate_params() raises on missing parameters
- `test_validate_unexpected_param_raises` — validate_params() raises on unexpected parameters
- `test_validate_bad_date_raises` — validate_params() raises on invalid date types
- `test_single_invocation` — initial_seed() dispatches single entry invocation
- `test_multiple_invocations` — initial_seed() dispatches multiple invocations
- `test_speculative_initial_seed_stores_templates` — initial_seed() stores Speculative templates instead of yielding requests
- `test_multiple_speculative_templates_same_entry` — initial_seed() stores multiple templates for the same entry
- `test_basemodel_param_dispatch` — initial_seed() validates and dispatches BaseModel params
- `test_empty_params_raises` — initial_seed() raises on empty params
- `test_none_params_raises` — initial_seed() raises on None params
- `test_unknown_entry_raises` — initial_seed() raises on unknown entry names
- `test_schema_structure` — schema() returns correct top-level structure
- `test_primitive_param_schema` — schema() maps primitives to JSON Schema types
- `test_basemodel_param_schema` — schema() references Pydantic models via $ref
- `test_speculative_schema_uses_pydantic_model` — schema() emits Speculative model's own schema
- `test_integer_param_schema` — schema() maps int to {"type": "integer"}
- `test_date_param_schema` — schema() maps date to {"type": "string", "format": "date"}
- `test_schema_is_json_serializable` — schema() output is JSON-serializable
- `test_tuple_param_rejected` — @entry rejects tuple parameters
- `test_unannotated_param_rejected` — @entry rejects unannotated parameters
- `test_unsupported_type_rejected` — @entry rejects unsupported types (e.g., list)

---

## `tests/speculation/`

### `test_speculative_protocol.py`
- `test_issubclass_with_pydantic_model` — issubclass(PydanticModel, Speculative) works for conforming models
- `test_isinstance_with_pydantic_instance` — isinstance check works on model instances
- `test_non_speculative_model_not_detected` — Plain BaseModel is not detected as Speculative
- `test_entry_auto_detects_speculative_param` — @entry sets speculative_param for Speculative params
- `test_entry_detects_non_speculative` — @entry leaves speculative_param as None for normal entries
- `test_is_speculative_defaults_to_false` — Request.is_speculative defaults to False
- `test_speculative_method_sets_3_tuple` — Request.speculative() sets 3-tuple speculation_id
- `test_speculative_preserves_fields` — Request.speculative() copies all other fields
- `test_discovers_from_templates` — SyncDriver discovers SpeculationState from stored templates
- `test_no_templates_no_state` — No templates means no speculation state
- `test_multiple_templates_same_entry` — Multiple templates create separate states with param_index
- `test_seeds_queue_with_correct_range` — Seeding iterates 1..to_int() with correct URLs
- `test_all_seeded_are_speculative_when_check_success_true` — All requests speculative when threshold=0
- `test_check_success_split` — IDs below threshold seeded as non-speculative, above as speculative
- `test_frozen_stops_after_seeding` — max_gap()==0 stops immediately after seeding
- `test_should_speculate_false_stops_after_seeding` — should_speculate()==False seeds but doesn't track
- `test_success_updates_highest` — Successful response updates highest_successful_id
- `test_failure_increments_consecutive` — Failed response increments consecutive_failures
- `test_stops_after_max_gap_failures` — Speculation stops when failures reach max_gap()
- `test_failure_below_watermark_ignored` — Failure for ID below highest_successful_id is ignored
- `test_non_speculative_request_ignored` — Non-speculative requests skip tracking
- `test_extends_when_near_ceiling` — Extension triggered when highest approaches ceiling
- `test_does_not_extend_when_far_from_ceiling` — No extension when highest is far from ceiling
- `test_frozen_not_extended` — Frozen partitions (max_gap==0) never extend
- `test_stopped_not_extended` — Already-stopped states don't extend
- `test_default_returns_true` — BaseScraper.fails_successfully() returns True by default
- `test_override_detects_soft_404` — Custom fails_successfully() detects soft-404 content
- `test_soft_404_treated_as_failure_in_tracking` — Soft-404 (200 + fails_successfully=False) counts as failure
- `test_stops_after_consecutive_failures` — End-to-end: driver stops extending after gap consecutive 404s
- `test_resets_failure_count_on_success` — End-to-end: interleaved successes reset failure counter

---

## `tests/parsing/`

### `test_lxml_page_element.py`
- `test_query_xpath_delegation` — query_xpath delegates to CheckedHtmlElement
- `test_query_xpath_returns_lxml_page_elements` — query_xpath wraps results as LxmlPageElement
- `test_query_xpath_strings` — query_xpath handles string results (text nodes)
- `test_query_css_delegation` — query_css delegates to CheckedHtmlElement
- `test_text_content` — text_content() returns element text
- `test_get_attribute` — get_attribute() returns element attributes
- `test_inner_html` — inner_html() returns serialized inner HTML
- `test_tag_name` — tag_name property returns the element tag
- `test_child_elements_inherit_observer` — Child elements share the parent's SelectorObserver
- `test_find_form_by_xpath` — find_form() locates a form by XPath selector
- `test_find_form_by_css` — find_form() locates a form by CSS selector
- `test_form_fields_extraction` — Form fields are extracted from input elements
- `test_form_action_resolution` — Form action URL is resolved against page URL
- `test_form_no_action_uses_base_url` — Form without action attribute uses the base page URL
- `test_find_links_by_xpath` — find_links() locates links by XPath selector
- `test_find_links_by_css` — find_links() locates links by CSS selector
- `test_find_links_resolves_urls` — find_links() resolves relative hrefs against page URL
- `test_find_links_skips_links_without_href` — find_links() skips anchor elements missing href
- `test_links_returns_all_links` — links property returns all links on the page
- `test_link_follow_creates_navigating_request` — Link.follow() creates a navigating Request with ViaLink
- `test_find_form_raises_on_no_match` — find_form() raises HTMLStructuralAssumptionException on no match
- `test_query_count_validation` — query_xpath/query_css enforce min_count/max_count constraints
- `test_link_selector_includes_position` — Link selector records position for disambiguation

### `test_page_element.py`
- `test_via_link_frozen` — ViaLink dataclass is immutable
- `test_via_form_submit_frozen` — ViaFormSubmit dataclass is immutable
- `test_form_field_frozen` — FormField dataclass is immutable
- `test_form_get_field` — Form.get_field() retrieves a field by name
- `test_form_submit_post` — Form.submit() creates a POST Request with form data
- `test_form_submit_with_overrides` — Form.submit() merges override data with existing fields
- `test_form_submit_with_submit_selector` — Form.submit() attaches ViaFormSubmit with submit selector
- `test_link_follow` — Link.follow() creates a navigating Request with ViaLink
- `test_link_frozen` — Link dataclass is immutable
- `test_form_frozen` — Form dataclass is immutable

### `test_selector_observer.py`
- `test_observer_is_plain_object` — SelectorObserver is a simple object (no metaclass magic)
- `test_record_simple_query` — Records a single selector query
- `test_record_nested_queries` — Records parent-child nested queries
- `test_deduplication_same_selector` — Deduplicates repeated identical selectors
- `test_sample_extraction` — Captures sample text from matched elements
- `test_simple_tree_output` — simple_tree() renders a text tree of queries
- `test_simple_tree_failure_indicator` — simple_tree() marks failed queries
- `test_json_output` — to_json() serializes query data
- `test_compose_absolute_selector_simple` — Composes absolute XPath from single query
- `test_compose_absolute_selector_nested_xpath` — Composes absolute XPath from nested XPath queries
- `test_compose_absolute_selector_mixed_types` — Composes absolute selector from mixed XPath/CSS
- `test_compose_absolute_selector_css` — Composes absolute selector from CSS queries
- `test_compose_absolute_selector_three_levels` — Composes absolute selector from three-level nesting
- `test_max_samples_limit` — Caps the number of recorded samples
- `test_max_sample_length` — Truncates long sample text

### `test_selector_utils.py`
- `test_css_selectors_always_compatible` — CSS selectors are always Playwright-compatible
- `test_element_targeting_xpath_compatible` — Element-targeting XPath is Playwright-compatible
- `test_text_node_xpath_incompatible` — text() XPath is incompatible with Playwright wait
- `test_attribute_xpath_incompatible` — @attribute XPath is incompatible with Playwright wait
- `test_exslt_functions_incompatible` — EXSLT functions are incompatible with Playwright wait
- `test_complex_element_xpath_compatible` — Complex element-targeting XPath is compatible
- `test_xpath_with_attribute_in_predicate_compatible` — XPath with attribute predicates is compatible
- `test_whitespace_handling` — Handles whitespace in selectors
- `test_mixed_cases` — Handles mixed-case function names
- `test_case_sensitivity` — Selector type matching is case-sensitive
- `test_attribute_in_middle_of_path` — Attribute in middle of XPath path is compatible

---

## `tests/playwright/`

### `test_page_identity.py`
- `test_direct_fingerprints_match` — [generative] Direct navigation: stored HTML matches the navigated URL across varying page/worker counts
- `test_vialink_fingerprints_match` — [generative] ViaLink navigation: stored HTML matches navigated URL across varying tree topology
- `test_wide_tree_siblings_share_parent` — [generative] Many siblings sharing a parent all get correct content
- `test_incidental_requests_not_cross_contaminated` — Incidental requests are attributed to the correct parent request
- `test_reset_does_not_leak_stale_incidentals` — Resetting incidental state does not leak stale entries

### `test_worker_page_registry.py`
- `test_worker_page_registry_invariants` — [generative] Random acquire/release/close sequences never violate registry invariants
- `test_concurrent_acquires_never_share_pages` — [generative] Concurrent acquires for distinct workers never return the same page
- `test_release_then_reacquire_gives_fresh_page` — Releasing and re-acquiring a worker gives a fresh page
