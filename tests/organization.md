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

### `test_accumulated_data.py`
- `test_base_request_has_accumulated_data_field` — BaseRequest has accumulated_data dict field
- `test_accumulated_data_can_be_set` — accumulated_data can be set at construction
- `test_accumulated_data_is_deep_copied` — accumulated_data is deep copied to prevent sharing
- `test_sibling_requests_have_independent_data` — Sibling requests get independent accumulated_data copies
- `test_nested_dict_mutations_do_not_propagate` — Nested dict mutations don't cross request boundaries
- `test_navigating_request_propagates_accumulated_data` — Navigating request carries accumulated_data forward
- `test_parse_appeals_list_adds_case_name_to_accumulated_data` — Scraper adds case_name to accumulated_data
- `test_full_scraping_pipeline_with_accumulated_data` — Integration: data flows through multi-page pipeline
- `test_accumulated_data_flows_through_three_pages` — Integration: data flows through three-page chain

### `test_archive_request.py`
- `test_archive_request_stores_url` — Archive request stores the target URL
- `test_archive_request_stores_continuation` — Archive request stores continuation name
- `test_archive_request_stores_expected_type` — Archive request stores expected file type (pdf, audio)
- `test_archive_request_expected_type_optional` — expected_type is optional (defaults to None)
- `test_archive_request_resolve_from_response` — Archive request resolves URL from Response context
- `test_archive_response_inherits_from_response` — ArchiveResponse inherits from Response
- `test_archive_response_stores_file_url` — ArchiveResponse stores local file path
- `test_archive_response_has_all_response_fields` — ArchiveResponse retains all Response fields
- `test_driver_creates_storage_directory` — SyncDriver creates the archive storage directory
- `test_driver_uses_temp_directory_by_default` — SyncDriver uses system temp dir when none provided
- `test_driver_saves_pdf_file` — SyncDriver downloads and saves PDF files
- `test_driver_saves_mp3_file` — SyncDriver downloads and saves MP3 files
- `test_driver_extracts_filename_from_url` — SyncDriver extracts filename from the URL path
- `test_save_file_method_with_explicit_filename` — Archive handler saves with explicit filename
- `test_save_file_method_generates_filename` — Archive handler generates filename when none in URL
- `test_save_file_method_handles_audio_type` — Archive handler handles audio file type
- `test_parse_detail_yields_archive_requests_for_opinions` — Scraper yields archive request for opinion PDFs
- `test_parse_detail_yields_archive_requests_for_oral_arguments` — Scraper yields archive request for oral argument audio
- `test_parse_detail_yields_parsed_data_when_no_files` — Scraper yields ParsedData when no files to archive
- `test_archive_opinion_yields_parsed_data_with_file_url` — Archive continuation yields ParsedData with local file path
- `test_full_scraping_pipeline_with_archive` — Integration: full pipeline downloads and archives files
- `test_archive_request_ancestry_preserved` — Archive request preserves previous_requests chain

### `test_permanent_data.py`
- `test_permanent_headers_persist_across_chain` — Permanent headers carry through the request chain
- `test_permanent_headers_inherited_by_children` — Child requests inherit parent's permanent headers
- `test_permanent_headers_merged_with_child` — Child's permanent headers merge with parent's
- `test_permanent_cookies_persist_across_chain` — Permanent cookies carry through the request chain
- `test_permanent_cookies_inherited_by_children` — Child requests inherit parent's permanent cookies
- `test_child_permanent_overrides_parent` — Child permanent values override parent's on conflict
- `test_permanent_supports_multiple_keys` — Permanent dict supports headers and cookies simultaneously
- `test_permanent_deep_copied_prevents_sharing` — Permanent dict is deep copied to prevent sharing
- `test_auth_token_flow` — Integration: auth token flows through permanent headers across chain

### `test_via_field.py`
- `test_via_field_on_base_request` — Request.via defaults to None
- `test_via_link_on_navigating_request` — ViaLink can be set on navigating Request
- `test_via_form_submit_on_navigating_request` — ViaFormSubmit can be set on navigating Request
- `test_via_propagates_through_resolve_from_response` — via propagates through resolve_from(Response)
- `test_via_propagates_through_nonnavigating_request` — via can be set on non-navigating Request
- `test_via_propagates_through_nonnavigating_resolve` — via propagates through non-navigating resolve
- `test_via_none_preserved` — via=None preserved through resolve_from
- `test_via_different_for_different_requests` — Sibling requests have independent via values
- `test_via_preserved_in_speculative_request` — via preserved through Request.speculative()

### `test_basescraper_introspection.py`
- `test_enum_values` — ScraperStatus enum has expected values
- `test_all_members` — ScraperStatus has all expected members
- `test_court_ids_defaults_to_empty_set` — BaseScraper.court_ids defaults to empty set
- `test_court_url_defaults_to_empty_string` — BaseScraper.court_url defaults to empty string
- `test_data_types_defaults_to_empty_set` — BaseScraper.data_types defaults to empty set
- `test_status_defaults_to_in_development` — BaseScraper.status defaults to IN_DEVELOPMENT
- `test_version_defaults_to_empty_string` — BaseScraper.version defaults to empty string
- `test_last_verified_defaults_to_empty_string` — BaseScraper.last_verified defaults to empty string
- `test_oldest_record_defaults_to_none` — BaseScraper.oldest_record defaults to None
- `test_requires_auth_defaults_to_false` — BaseScraper.requires_auth defaults to False
- `test_rate_limits_defaults_to_none` — BaseScraper.rate_limits defaults to None
- `test_ssl_context_defaults_to_none` — BaseScraper.ssl_context defaults to None
- `test_court_ids_override` — Subclass can override court_ids
- `test_court_url_override` — Subclass can override court_url
- `test_data_types_override` — Subclass can override data_types
- `test_status_override` — Subclass can override status
- `test_version_override` — Subclass can override version
- `test_last_verified_override` — Subclass can override last_verified
- `test_oldest_record_override` — Subclass can override oldest_record
- `test_requires_auth_override` — Subclass can override requires_auth
- `test_rate_limits_override` — Subclass can override rate_limits
- `test_overrides_do_not_affect_base_class` — Subclass overrides don't affect base class
- `test_get_params_returns_none_by_default` — get_params() returns None by default
- `test_get_params_returns_provided_params` — get_params() returns constructor params
- `test_get_params_returns_arbitrary_object` — get_params() accepts arbitrary objects
- `test_default_returns_none` — get_ssl_context() returns None by default
- `test_custom_returns_ssl_context` — get_ssl_context() returns custom SSLContext
- `test_ssl_context_classvar_returned_when_set` — ssl_context ClassVar returned when set
- `test_discovers_all_step_methods` — list_steps() discovers all @step methods
- `test_returns_step_info_objects` — list_steps() returns StepInfo objects
- `test_step_priority_metadata` — StepInfo records priority
- `test_step_encoding_metadata` — StepInfo records encoding
- `test_excludes_non_step_methods` — list_steps() excludes non-@step methods
- `test_empty_scraper_returns_empty_list` — list_steps() returns empty list for bare scraper

---

## `tests/drivers/async/`

### `test_async_driver.py`
- `test_async_driver_processes_single_request` — AsyncDriver processes a single request
- `test_async_driver_processes_multiple_pages` — AsyncDriver processes multi-page scraper
- `test_driver_stops_when_event_set_before_start` — AsyncDriver stops when stop_event set before run
- `test_driver_completes_when_stop_event_not_set` — AsyncDriver completes normally without stop_event
- `test_single_worker_processes_all_requests` — Single worker processes all requests
- `test_multiple_workers_process_all_requests` — Multiple workers process all requests
- `test_priority_ordering_with_single_worker` — Priority ordering respected with single worker
- `test_archive_request_saves_file` — AsyncDriver handles archive requests
- `test_lifecycle_callbacks_fire` — on_run_start and on_run_complete fire

---

## `tests/drivers/sync/`

### `test_callbacks.py`
- `test_driver_accepts_on_data_callback` — SyncDriver constructor accepts on_data callback
- `test_driver_accepts_none_callback` — SyncDriver accepts None for on_data
- `test_callback_invoked_when_data_yielded` — on_data fires when ParsedData is yielded
- `test_callback_not_invoked_if_none` — No callback invocation when on_data is None
- `test_callback_receives_unwrapped_data` — on_data receives unwrapped data (not ParsedData wrapper)
- `test_multiple_data_items_invoke_callback_multiple_times` — on_data fires once per ParsedData
- `test_saves_data_to_jsonl_file` — save_to_jsonl_file callback writes JSONL
- `test_flushes_data_immediately` — save_to_jsonl_file flushes after each write
- `test_saves_data_to_file_path` — save_to_jsonl_path callback writes to a Path
- `test_accepts_string_path` — save_to_jsonl_path accepts string paths
- `test_invokes_all_callbacks` — combine_callbacks invokes all wrapped callbacks
- `test_combines_save_and_count` — combine_callbacks works with save + count together
- `test_counts_data_items` — count_data increments a counter per item
- `test_creates_counter_if_none` — count_data creates a counter if none provided
- `test_callback_with_archive_requests` — on_data fires for data from archive continuations
- `test_callback_error_does_not_stop_scraping` — Callback exceptions don't halt the driver

### `test_lifecycle_hooks.py`
- `test_run_start_hook_is_called` — on_run_start fires at beginning of run()
- `test_run_start_receives_correct_scraper_name` — on_run_start receives the scraper class name
- `test_run_complete_hook_is_called` — on_run_complete fires at end of run()
- `test_run_complete_status_is_completed_on_success` — on_run_complete status is "completed" on success
- `test_run_complete_status_is_error_on_exception` — on_run_complete status is "error" on exception
- `test_run_complete_hook_fires_in_finally_block` — on_run_complete fires even when exceptions occur
- `test_both_hooks_fire_in_order` — on_run_start fires before on_run_complete

### `test_structural_errors.py`
- `test_exception_has_required_attributes` — ScraperAssumptionException stores message, url, context
- `test_exception_context_defaults_to_empty_dict` — Context defaults to empty dict
- `test_exception_formats_message_with_url` — Message includes request URL
- `test_exception_formats_message_with_context` — Message includes context details
- `test_exception_has_required_attributes` (HTML) — HTMLStructuralAssumptionException stores selector, counts
- `test_exception_formats_expected_count_at_least` — Formats "at least N" message
- `test_exception_formats_expected_count_exactly` — Formats "exactly N" message
- `test_exception_formats_expected_count_between` — Formats "between N and M" message
- `test_exception_includes_context_dict` — Exception includes context dict
- `test_checked_xpath_returns_results_when_count_matches` — CheckedHtmlElement.checked_xpath passes when count in range
- `test_checked_xpath_raises_when_count_below_min` — checked_xpath raises when too few results
- `test_checked_xpath_raises_when_count_above_max` — checked_xpath raises when too many results
- `test_checked_xpath_defaults_to_min_count_one` — checked_xpath defaults min_count to 1
- `test_checked_xpath_allows_unlimited_max_count` — checked_xpath allows unlimited max when not set
- `test_checked_css_returns_results_when_count_matches` — checked_css passes when count in range
- `test_checked_css_raises_when_count_below_min` — checked_css raises when too few results
- `test_checked_css_raises_when_count_above_max` — checked_css raises when too many results
- `test_checked_css_handles_invalid_selector` — checked_css handles malformed selectors
- `test_element_delegation_works` — CheckedHtmlElement delegates to lxml element
- `test_request_url_is_optional` — CheckedHtmlElement works without a request URL
- `test_nested_xpath_queries_work` — Nested XPath queries on child elements work
- `test_nested_xpath_raises_on_count_mismatch` — Nested XPath raises on count mismatch
- `test_nested_css_queries_work` — Nested CSS queries on child elements work
- `test_nested_css_raises_on_count_mismatch` — Nested CSS raises on count mismatch
- `test_mixed_xpath_css_nesting` — Mixed XPath and CSS nesting works
- `test_deeply_nested_queries` — Three-level deep nesting works
- `test_request_url_propagates_through_nesting` — Request URL propagates through nested elements
- `test_xpath_text_results_are_strings_not_checkable` — XPath text() results are plain strings
- `test_xpath_attribute_results_are_strings_not_checkable` — XPath @attr results are plain strings
- `test_error_page_returns_different_structure` — Mock server error page has different HTML structure
- `test_error_page_includes_reference_docket` — Error page includes a reference docket
- `test_error_page_has_error_code` — Error page includes error code
- `test_scraper_raises_exception_on_structural_change` — Scraper raises on unexpected HTML structure
- `test_normal_page_does_not_raise_exception` — Scraper succeeds on expected HTML structure
- `test_callback_receives_exception_and_can_stop` — on_structural_error receives exception, returns False to stop
- `test_callback_can_continue_scraping` — on_structural_error returns True to continue
- `test_no_callback_raises_exception` — Without callback, exception propagates
- `test_log_structural_error_and_stop_callback` — Built-in log-and-stop callback works

### `test_transient_exceptions.py`
- `test_transient_exception_is_exception_subclass` — TransientException inherits from Exception
- `test_transient_exception_can_be_raised` — TransientException can be raised and caught
- `test_transient_exception_message` — TransientException stores message
- `test_exception_has_required_attributes` (HTTPResponse) — HTMLResponseAssumptionException stores status, expected codes
- `test_exception_is_transient` (HTTPResponse) — HTMLResponseAssumptionException is a TransientException
- `test_exception_message_includes_status_code` — Message includes actual status code
- `test_exception_message_includes_expected_codes` — Message includes expected codes
- `test_exception_message_with_single_expected_code` — Message formats single expected code
- `test_exception_message_with_multiple_expected_codes` — Message formats multiple expected codes
- `test_exception_has_required_attributes` (Timeout) — RequestTimeoutException stores url, timeout
- `test_exception_is_transient` (Timeout) — RequestTimeoutException is a TransientException
- `test_exception_message_includes_url` — Timeout message includes URL
- `test_exception_message_includes_timeout` — Timeout message includes timeout value
- `test_exception_formats_timeout_with_decimal` — Timeout formats decimal seconds
- `test_server_error_endpoint_returns_500` — Mock server returns 500 for error endpoint
- `test_server_error_endpoint_returns_error_html` — Mock server returns error HTML
- `test_server_error_endpoint_includes_reference` — Mock error page includes reference ID
- `test_normal_endpoint_returns_200` — Mock server returns 200 for normal endpoint
- `test_driver_raises_exception_on_500_error` — SyncDriver raises on 500 response
- `test_driver_does_not_raise_exception_on_200` — SyncDriver does not raise on 200 response
- `test_driver_raises_exception_on_503_error` — SyncDriver raises on 503 response
- `test_exception_includes_request_url` — Exception includes the request URL
- `test_callback_receives_exception_and_can_stop` — on_transient_exception receives exception, returns False to stop
- `test_callback_can_continue_scraping` — on_transient_exception returns True to continue
- `test_no_callback_raises_exception` — Without callback, transient exception propagates
- `test_timeout_exception_triggers_callback` — Timeout exception triggers on_transient_exception

### `test_priority_queue.py`
- `test_lower_priority_number_processed_first` — Lower priority number is processed first
- `test_archive_requests_have_priority_1` — Archive requests default to priority 1
- `test_fifo_ordering_within_same_priority` — FIFO order maintained within same priority level
- `test_priority_preserved_through_resolution` — Priority preserved through request resolution
- `test_high_priority_requests_clear_queue_faster` — High-priority requests clear before low-priority
- `test_base_request_default_priority_is_9` — BaseRequest defaults to priority 9
- `test_archive_request_default_priority_is_1` — Archive Request defaults to priority 1

### `test_deduplication.py`
- `test_same_url_produces_same_key` — Same URL generates same deduplication key
- `test_different_url_produces_different_key` — Different URLs generate different keys
- `test_same_url_different_params_produces_different_key` — Same URL with different query params generates different keys
- `test_same_params_different_order_produces_same_key` — Same params in different order generate same key
- `test_post_data_dict_sorted_consistently` — POST dict data is sorted for consistent hashing
- `test_post_data_list_sorted_consistently` — POST list data is sorted for consistent hashing
- `test_json_data_sorted_consistently` — JSON data is sorted for consistent hashing
- `test_custom_dedup_key_overrides_default` — Custom deduplication_key overrides the auto-generated one
- `test_custom_dedup_key_preserved_through_resolution` — Custom key survives request resolution
- `test_duplicate_check_prevents_enqueueing_duplicates` — duplicate_check callback prevents re-enqueueing
- `test_no_duplicate_check_allows_all_requests` — Without callback, all requests are enqueued
- `test_duplicate_check_with_different_keys` — Different dedup keys are not considered duplicates
- `test_duplicate_check_with_custom_keys` — Custom keys work with duplicate_check callback
- `test_skip_dedup_bypasses_duplicate_check` — SkipDeduplicationCheck bypasses the callback
- `test_skip_dedup_not_tracked_in_seen_keys` — Skipped requests aren't recorded in seen keys
- `test_skip_dedup_isinstance_check` — SkipDeduplicationCheck is identified via isinstance

### `test_data_validation.py`
- `test_exception_has_required_attributes` — DataFormatAssumptionException stores model_name, errors, failed_doc
- `test_exception_formats_message_with_error_summary` — Exception message summarizes validation errors
- `test_exception_includes_context_dict` — Exception includes context dict
- `test_valid_data_passes_validation` — Valid Pydantic data passes validation
- `test_missing_required_field_raises_validation_error` — Missing required field raises ValidationError
- `test_invalid_field_type_raises_validation_error` — Wrong field type raises ValidationError
- `test_deferred_validation_stores_data_and_model` — DeferredValidation stores raw data and model class
- `test_confirm_validates_valid_data` — DeferredValidation.confirm() succeeds for valid data
- `test_confirm_raises_data_format_exception_for_invalid_data` — confirm() raises DataFormatAssumptionException for invalid data
- `test_raw_data_returns_copy` — DeferredValidation.raw_data returns a copy
- `test_scraper_validates_and_yields_valid_data` — Scraper validates and yields valid data via driver
- `test_driver_raises_exception_for_invalid_data` — Driver raises on invalid data without callback
- `test_on_invalid_data_callback_receives_invalid_data` — on_invalid_data callback receives the DeferredValidation
- `test_default_invalid_data_callback_logs_error` — Default invalid data callback logs the error

### `test_archive_handler.py`
- `test_extracts_filename_from_url` — LocalSyncArchiveHandler extracts filename from URL
- `test_generates_filename_when_no_path` — Handler generates filename when URL has no path
- `test_uses_pdf_extension` — Handler uses .pdf extension for pdf type
- `test_uses_mp3_extension` — Handler uses .mp3 extension for audio type
- `test_should_download_true_without_dedup_key` — should_download returns True without dedup key
- `test_should_download_true_when_dedup_dir_missing` — should_download returns True when dedup dir missing
- `test_should_download_true_when_dedup_dir_empty` — should_download returns True when dedup dir empty
- `test_should_download_false_when_dedup_dir_has_files` — should_download returns False when file already exists
- `test_save_with_dedup_key_creates_subdirectory` — save() creates subdirectory from dedup key
- `test_should_download_always_false` — NoDownloadsSyncArchiveHandler always returns False
- `test_custom_handler_receives_parameters` — Custom handler receives correct save parameters
- `test_custom_handler_return_value_used_as_file_url` — Custom handler return value becomes file_url
- `test_should_download_false_skips_fetch` — Handler returning False skips the HTTP fetch
- `test_default_handler_used_when_none_provided` — SyncDriver uses LocalSyncArchiveHandler by default
- `test_handler_called_for_each_archive_request` — Handler called once per archive request
- `test_dedup_key_creates_subdirectory_in_file_path` — Dedup key creates subdirectory in sync driver
- `test_dedup_key_creates_subdirectory_async_driver` — Dedup key creates subdirectory in async driver
- `test_dedup_key_creates_subdirectory_persistent_driver` — Dedup key creates subdirectory in persistent driver
- `test_dedup_key_creates_subdirectory_playwright_driver` — Dedup key creates subdirectory in Playwright driver

### `test_stop_event.py`
- `test_driver_stops_when_event_set_before_start` — SyncDriver stops when stop_event set before run
- `test_driver_completes_when_stop_event_not_set` — SyncDriver completes normally without stop_event
- `test_driver_works_without_stop_event` — SyncDriver works when no stop_event provided
- `test_driver_stops_after_current_request` — SyncDriver finishes current request then stops

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

### `test_step.py`
- `test_response_injected` — @step injects Response when param named "response"
- `test_request_injected` — @step injects Request when param named "request"
- `test_previous_request_injected` — @step injects previous_request from chain
- `test_previous_request_none_for_entry` — previous_request is None for entry requests
- `test_json_content_injected` — @step parses and injects json_content
- `test_json_parsing_failure_raises_exception` — JSON parse failure raises ScraperAssumptionException
- `test_lxml_tree_injected` — @step parses and injects lxml_tree as CheckedHtmlElement
- `test_text_injected` — @step injects response text
- `test_callable_continuation_resolved_to_string` — Callable continuations resolved to function name string
- `test_default_priority_is_nine` — @step default priority is 9
- `test_custom_priority_attached` — @step(priority=N) attaches custom priority
- `test_is_step_returns_true` — is_step() returns True for decorated methods
- `test_is_step_returns_false_for_undecorated` — is_step() returns False for plain methods
- `test_callable_continuation_inherits_target_priority` — Yielded request inherits target step's priority
- `test_default_encoding_is_utf8` — Default encoding is utf-8
- `test_custom_encoding_attached` — @step(encoding=...) attaches custom encoding
- `test_default_xsd_is_none` — Default XSD is None
- `test_xsd_attached_when_specified` — @step(xsd=...) attaches XSD path
- `test_xsd_with_other_parameters` — XSD coexists with other @step parameters
- `test_default_json_model_is_none` — Default json_model is None
- `test_json_model_attached_when_specified` — @step(json_model=...) attaches model path
- `test_json_model_with_other_parameters` — json_model coexists with other @step parameters
- `test_json_model_and_xsd_together` — json_model and xsd can be set together
- `test_multiple_injections_work_together` — Multiple param injections work simultaneously
- `test_decorator_without_parens` — @step (no parens) works
- `test_decorator_with_parens` — @step() (empty parens) works
- `test_local_filepath_injected_for_archive_response` — local_filepath injected from ArchiveResponse
- `test_local_filepath_none_for_regular_response` — local_filepath is None for regular Response
- `test_accumulated_data_injected` — @step injects accumulated_data from request

### `test_single_page.py`
- `test_parses_html` — single_page() parses HTML and returns ParsedData items
- `test_custom_url` — single_page() accepts custom URL
- `test_multiple_results` — single_page() collects multiple ParsedData items
- `test_parses_json` — single_page() works with JSON content
- `test_passes_accumulated_data` — single_page() passes accumulated_data to step
- `test_default_empty_accumulated_data` — single_page() defaults to empty accumulated_data
- `test_filters_non_parsed_data` — single_page() filters out non-ParsedData yields

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

## `tests/demo/`

### `test_demo.py`
- `test_homepage` — Demo website serves homepage
- `test_cases_list` — Demo website serves cases list page
- `test_case_detail` — Demo website serves case detail page
- `test_case_not_found` — Demo website returns soft-404 for invalid case
- `test_opinions_list` — Demo website serves opinions list
- `test_opinion_detail` — Demo website serves opinion detail
- `test_oral_arguments_list` — Demo website serves oral arguments list
- `test_oral_argument_detail` — Demo website serves oral argument detail
- `test_justices_html` — Demo website serves justices HTML page
- `test_justices_json_api` — Demo website serves justices JSON API
- `test_justice_detail_json` — Demo website serves single justice JSON
- `test_scraper_extracts_all_cases` — Demo scraper finds all 30 cases via speculation
- `test_scraper_extracts_justices` — Demo scraper extracts all justices from JSON API
- `test_fixture_matches_cases` — expected_output.json fixture matches data module
- `test_case_data_matches_fixture` — Scraped case data matches fixture file

---

## `tests/rate_limiting/`

### `test_rate_limiting.py`
- `test_rate_limit_respected` (Sync) — SyncDriver respects declared rate limits
- `test_rate_limit_respected` (Async) — AsyncDriver respects declared rate limits
- `test_rate_limit_respected` (Persistent) — PersistentDriver respects declared rate limits
- `test_rate_limit_respected` (Playwright) — PlaywrightDriver respects declared rate limits
- `test_put_accepts_items_within_limit` — AioSQLiteBucket accepts items within rate window
- `test_put_rejects_when_limit_exceeded` — AioSQLiteBucket rejects when limit exceeded
- `test_put_sets_failing_rate` — AioSQLiteBucket sets failing_rate on rejection
- `test_put_accepts_after_window_expires` — AioSQLiteBucket accepts after rate window expires
- `test_bypass_skips_rate_limit` (Sync) — SyncDriver bypass_rate_limit skips the limiter
- `test_bypass_skips_rate_limit` (Async) — AsyncDriver bypass_rate_limit skips the limiter
- `test_bypass_skips_rate_limit` (Persistent) — PersistentDriver bypass_rate_limit skips the limiter
- `test_bypass_skips_rate_limit` (Playwright) — PlaywrightDriver bypass_rate_limit skips the limiter
- `test_archive_skip_bypasses_rate_limiter` (Persistent) — PersistentDriver archive skip bypasses limiter
- `test_archive_skip_bypasses_rate_limiter` (Playwright) — PlaywrightDriver archive skip bypasses limiter

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

### `test_xpath_observer.py`
- `test_basic_recording` — Records a basic XPath query
- `test_multiple_queries` — Records multiple XPath queries
- `test_nested_queries` — Records nested parent-child queries
- `test_css_recording` — Records CSS selector queries
- `test_simple_tree_format` — simple_tree() renders indented text tree
- `test_simple_tree_failure_markers` — simple_tree() marks failed queries with indicator
- `test_json_serialization` — json() serializes query data
- `test_no_observer_active` — Queries work when no observer is active
- `test_context_manager_cleanup` — Context manager cleans up observer on exit
- `test_nested_contexts_not_supported` — Nested observer contexts are not supported
- `test_sample_truncation` — Long sample text is truncated
- `test_max_samples_limit` — Number of recorded samples is capped
- `test_sample_extracts_text_content` — Samples extract text_content from elements
- `test_sample_handles_string_results` — Samples handle string XPath results
- `test_expected_counts_recorded` — Expected min/max counts recorded on queries
- `test_root_query_has_no_parent` — Root-level query has no parent element
- `test_child_query_has_parent_id` — Child query records parent element ID
- `test_deeply_nested_parent_chain` — Deeply nested queries form parent chain
- `test_sibling_queries_same_parent` — Sibling queries share same parent
- `test_same_selector_different_elements_deduplicated` — Same selector on different elements deduplicated
- `test_parent_element_id_in_json` — Parent element ID appears in JSON output
- `test_css_queries_track_parent` — CSS queries track parent elements
- `test_parent_child_single_iteration` — Dedup: parent+child single iteration
- `test_parent_child_child_two_iterations` — Dedup: parent+child across two iterations
- `test_parent_child_times_three` — Dedup: parent+child across three iterations
- `test_different_selectors_not_deduplicated` — Different selectors are not deduplicated
- `test_same_selector_different_parents_not_deduplicated` — Same selector with different parents not deduplicated
- `test_dedup_aggregates_samples_up_to_max` — Dedup aggregates samples up to max
- `test_dedup_preserves_first_description` — Dedup preserves first occurrence's description
- `test_dedup_works_with_css_selectors` — Dedup works for CSS selectors

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

### `test_form_submit.py`
- `test_form_submit_filters_cases` — PlaywrightDriver submits form and filters results
- `test_complex_form_with_hidden_and_radio_fields` — PlaywrightDriver handles hidden and radio form fields

### `test_tab_forking.py`
- `test_session_tree_full_scrape` — Tab forking: full scrape with per-request pages and route interception
- `test_session_tree_resume_with_cookies` — Tab forking: resume with cookies across forked pages
- `test_same_url_post_not_intercepted` — POST to same URL is not intercepted by route handler

### `test_driver_integration.py`
- `test_basic_navigation` — PlaywrightDriver navigates pages and extracts data
- `test_await_list_wait_for_load_state` — PlaywrightDriver respects WaitForLoadState conditions
- `test_await_list_wait_for_selector` — PlaywrightDriver respects WaitForSelector conditions
- `test_dom_snapshot_model` — PlaywrightDriver captures DOM snapshot model
- `test_browser_config_persistence` — PlaywrightDriver persists browser config across requests

### `test_step_playwright_features.py`
- `test_page_injected_with_observer` — @step injects page (PageElement) with SelectorObserver
- `test_page_and_lxml_tree_coexist` — page and lxml_tree can be injected simultaneously
- `test_observer_accessible_after_execution` — SelectorObserver accessible on metadata after step runs
- `test_default_await_list_is_empty` — @step default await_list is empty
- `test_await_list_single_condition` — @step(await_list=[...]) stores single wait condition
- `test_await_list_multiple_conditions` — @step(await_list=[...]) stores multiple wait conditions
- `test_await_list_with_url_condition` — await_list supports WaitForURL condition
- `test_await_list_with_other_parameters` — await_list coexists with priority and encoding
- `test_default_auto_await_timeout_is_none` — @step default auto_await_timeout is None
- `test_auto_await_timeout_set` — @step(auto_await_timeout=N) stores timeout
- `test_auto_await_timeout_with_await_list` — auto_await_timeout coexists with await_list
- `test_auto_await_timeout_various_values` — auto_await_timeout accepts various integer values
- `test_page_with_await_list_and_auto_await` — page, await_list, and auto_await_timeout together
- `test_backward_compatibility_maintained` — @step without Playwright params still works

### `test_wait_conditions.py`
- `test_basic_construction` (Selector) — WaitForSelector stores selector string
- `test_construction_with_state` (Selector) — WaitForSelector stores state param
- `test_construction_with_timeout` (Selector) — WaitForSelector stores timeout
- `test_frozen_dataclass` (Selector) — WaitForSelector is immutable
- `test_valid_states` (Selector) — WaitForSelector accepts valid state values
- `test_basic_construction` (LoadState) — WaitForLoadState stores state
- `test_construction_with_state` (LoadState) — WaitForLoadState accepts different states
- `test_construction_with_timeout` (LoadState) — WaitForLoadState stores timeout
- `test_frozen_dataclass` (LoadState) — WaitForLoadState is immutable
- `test_valid_states` (LoadState) — WaitForLoadState accepts valid state values
- `test_basic_construction` (URL) — WaitForURL stores URL pattern
- `test_construction_with_timeout` (URL) — WaitForURL stores timeout
- `test_frozen_dataclass` (URL) — WaitForURL is immutable
- `test_construction_with_pattern` — WaitForURL accepts regex patterns
- `test_basic_construction` (Timeout) — WaitForTimeout stores timeout value
- `test_construction_with_various_timeouts` — WaitForTimeout accepts various values
- `test_frozen_dataclass` (Timeout) — WaitForTimeout is immutable
- `test_zero_timeout` — WaitForTimeout accepts zero
- `test_await_list_simulation` — Multiple conditions compose in a list
- `test_mixed_wait_conditions_with_timeouts` — Mixed condition types with timeouts

---

## `tests/persistent_driver/`

### `core/test_playwright_db_persistence.py`
- `test_schema_includes_incidental_requests_table` — Schema has incidental_requests table
- `test_schema_includes_browser_config_json_field` — Schema has browser_config_json field
- `test_schema_version_is_17` — Schema version is 17
- `test_insert_incidental_request` — Can insert and retrieve incidental requests
- `test_get_incidental_requests_by_parent` — Can query incidental requests by parent ID
- `test_browser_config_persistence` — Browser config persists across sessions

### `debugger/test_inspection.py`
- `test_open_read_only` — Opens debugger in read-only mode
- `test_open_write_mode` — Opens debugger in write mode
- `test_open_with_string_path` — Opens debugger with a string path instead of Path
- `test_get_run_metadata` — Retrieves run metadata from populated database
- `test_get_run_metadata_empty_db` — Returns None for empty database
- `test_get_run_status_running` — Reports running status for active scraper
- `test_get_run_status_completed` — Reports completed status for finished scraper
- `test_get_run_status_empty_db` — Reports unknown status for empty database
- `test_get_stats` — Retrieves comprehensive statistics (queue, throughput, compression, results, errors)
- `test_list_requests_no_filter` — Lists all requests without filtering
- `test_list_requests_filter_by_status` — Filters requests by status (completed, pending, failed)
- `test_list_requests_filter_by_continuation` — Filters requests by continuation name
- `test_list_requests_pagination` — Paginates request listing with limit/offset
- `test_get_request` — Gets a single request by ID
- `test_get_request_not_found` — Returns None for non-existent request
- `test_get_request_summary` — Gets request counts grouped by continuation and status
- `test_list_responses` — Lists all responses
- `test_list_responses_filter_by_continuation` — Filters responses by continuation
- `test_get_response` — Gets a single response by ID
- `test_get_response_content` — Gets decompressed response content
- `test_list_errors` — Lists all errors
- `test_list_errors_filter_by_type` — Filters errors by type (xpath, etc.)
- `test_list_errors_filter_by_resolution` — Filters errors by resolved/unresolved
- `test_get_error` — Gets a single error by ID
- `test_get_error_summary` — Gets error counts by type, continuation, and totals
- `test_list_results` — Lists all results
- `test_list_results_filter_by_validity` — Filters results by valid/invalid
- `test_get_result` — Gets a single result by ID
- `test_get_result_summary` — Gets result counts by type and validity
- `test_get_speculation_summary` — Gets speculation progress and tracking summary
- `test_get_speculative_progress` — Gets speculative progress dictionary
- `test_get_rate_limiter_state` — Gets rate limiter state (None when no rate items)
- `test_get_throughput_stats` — Gets throughput statistics
- `test_get_compression_stats` — Gets compression statistics with totals
- `test_list_compression_dicts` — Lists compression dictionaries with metadata

### `debugger/test_manipulation.py`
- `test_cancel_request_read_only` — cancel_request raises PermissionError in read-only mode
- `test_cancel_requests_by_continuation_read_only` — cancel_requests_by_continuation raises PermissionError in read-only mode
- `test_resolve_error_read_only` — resolve_error raises PermissionError in read-only mode
- `test_train_compression_dict_read_only` — train_compression_dict raises PermissionError in read-only mode
- `test_recompress_responses_read_only` — recompress_responses raises PermissionError in read-only mode
- `test_cancel_request` — Cancels a pending request (marks as failed)
- `test_cancel_requests_by_continuation` — Cancels all pending/held requests for a continuation
- `test_resolve_error` — Resolves an error with notes
- `test_export_results_jsonl` — Exports results to JSONL format
- `test_export_results_jsonl_filtered` — Exports filtered (valid-only) results to JSONL
- `test_diagnose_error` — Diagnoses error (raises ValueError when response missing)
- `test_diagnose_error_not_found` — Diagnoses non-existent error (raises ValueError)
- `test_search_text_match` — Text search finds matching responses
- `test_search_text_case_insensitive` — Text search is case insensitive
- `test_search_text_no_match` — Text search returns empty for no matches
- `test_search_regex_match` — Regex search finds matching responses
- `test_search_regex_no_match` — Regex search returns empty for no matches
- `test_search_xpath_match` — XPath search finds matching responses
- `test_search_xpath_no_match` — XPath search returns empty for no matches
- `test_search_with_continuation_filter` — Search with continuation filter narrows results
- `test_search_requires_exactly_one_pattern` — Search raises ValueError without exactly one pattern
- `test_search_returns_correct_ids` — Search returns correct response and request IDs
- `test_seed_speculative_requests_creates_pending_requests` — Seeding creates pending requests with correct URLs and continuations
- `test_seed_speculative_requests_requires_write_mode` — Seeding raises PermissionError in read-only mode
- `test_seed_speculative_requests_fails_for_non_speculate_function` — Seeding fails for non-speculative entry functions
- `test_seed_speculative_requests_fails_for_nonexistent_step` — Seeding fails when step doesn't exist on scraper
- `test_seed_speculative_requests_fails_for_unknown_scraper` — Seeding fails when scraper not found in registry

### `debugger/test_comparison.py`
- `test_identical_strings` — Identical strings have Levenshtein distance 0
- `test_empty_string` — Distance to empty string is length of other string
- `test_single_substitution` — Single character substitution has distance 1
- `test_single_insertion` — Single character insertion has distance 1
- `test_single_deletion` — Single character deletion has distance 1
- `test_multiple_changes` — Multiple changes accumulate Levenshtein distance
- `test_completely_different` — Completely different strings have max distance
- `test_empty_lists` — Empty lists return empty pairing
- `test_only_original` — All original results unpaired when new is empty
- `test_only_new` — All new results unpaired when original is empty
- `test_perfect_match` — Identical results are paired correctly
- `test_close_match` — Results with small differences are paired
- `test_multiple_results_greedy_pairing` — Greedy pairing chooses closest match first
- `test_unequal_counts_some_unpaired` — Extra results remain unpaired
- `test_identical_dicts` — Identical dicts have no diffs
- `test_changed_value` — Changed values are reported
- `test_added_field` — Added fields are reported
- `test_removed_field` — Removed fields are reported
- `test_multiple_changes` — Multiple dict changes are all reported
- `test_no_errors` — No errors in either execution yields no_change
- `test_error_introduced` — Error present in new but not original is introduced
- `test_error_resolved` — Error present in original but not new is resolved
- `test_same_error` — Same error in both executions is no_change
- `test_error_changed_type` — Error type change is detected
- `test_error_changed_message` — Error message change is detected
- `test_identical_outputs` — Completely identical continuation outputs
- `test_request_added` — New code generates additional request
- `test_request_removed` — New code generates fewer requests
- `test_data_added` — New code yields more data
- `test_data_removed` — New code yields less data
- `test_data_changed` — Paired data has field-level changes
- `test_error_introduced` — New code raises error (full comparison)
- `test_error_resolved` — New code fixes error (full comparison)
- `test_empty_summary` — Empty summary has all zeros
- `test_add_identical_result` — Adding identical result updates counters
- `test_add_result_with_request_changes` — Request changes update summary counters
- `test_add_result_with_data_changes` — Data changes update summary counters
- `test_add_result_with_error_introduced` — Error introduction updates summary counters
- `test_add_multiple_results` — Multiple results aggregate correctly in summary

### `debugger/test_debugger_comparison.py`
- `test_get_child_requests_transitive` — Gets child requests transitively (children and grandchildren)
- `test_get_results_for_request` — Gets all results stored for a request
- `test_sample_terminal_requests` — Samples terminal requests (no children) for a continuation
- `test_compare_continuation_identical_output` — compare_continuation detects identical outputs
- `test_compare_continuation_with_data_changes` — compare_continuation detects field-level data changes
- `test_compare_continuation_with_request_changes` — compare_continuation detects added/removed child requests
- `test_compare_continuation_with_error` — compare_continuation detects newly introduced errors
- `test_compare_continuation_missing_response` — compare_continuation raises ValueError when response missing

### `debugger/test_dry_run.py`
- `test_dry_run_captures_data_and_requests` — DryRunDriver captures both ParsedData and child requests from a continuation
- `test_dry_run_captures_archive_request` — DryRunDriver captures archive Request with expected_type and continuation
- `test_dry_run_captures_error` — DryRunDriver captures errors raised during continuation execution
- `test_dry_run_reconstructs_context` — DryRunDriver reconstructs accumulated/aux/permanent context from stored data
- `test_dry_run_no_network_io` — DryRunDriver captures requests without performing any network I/O

### `debugger/test_integrity.py`
- `test_check_integrity_no_issues` — check_integrity reports no issues when all completed requests have responses
- `test_check_integrity_orphaned_request` — check_integrity detects completed requests without responses
- `test_check_integrity_no_orphaned_responses` — Orphaned responses cannot exist in merged model
- `test_check_integrity_orphaned_request_only` — check_integrity detects orphaned request (no orphaned responses possible)
- `test_get_orphan_details_no_orphans` — get_orphan_details returns empty when no orphans
- `test_get_orphan_details_with_orphaned_request` — get_orphan_details includes request details for orphaned requests
- `test_get_orphan_details_no_orphaned_responses` — get_orphan_details returns empty orphaned_responses in merged model
- `test_get_ghost_requests_no_ghosts` — No ghost requests when completed requests have results
- `test_get_ghost_requests_detects_ghost` — Detects ghost request (completed, no children, no results)
- `test_get_ghost_requests_not_ghost_with_children` — Requests with children are not ghosts
- `test_get_ghost_requests_not_ghost_with_results` — Requests with results are not ghosts
- `test_get_ghost_requests_multiple_continuations` — Ghost requests grouped by continuation
- `test_get_ghost_requests_pending_not_ghost` — Pending requests are not considered ghosts
- `test_get_ghost_requests_failed_not_ghost` — Failed requests are not considered ghosts
- `test_check_estimates_no_estimates` — check_estimates returns zeros when no estimates exist
- `test_check_estimates_passing` — check_estimates passes when actual count matches expected
- `test_check_estimates_failing_too_few` — check_estimates fails when too few results produced
- `test_check_estimates_failing_too_many` — check_estimates fails when too many results produced
- `test_check_estimates_unbounded_max` — check_estimates passes with unbounded max when min is met
- `test_check_estimates_deep_descendants` — check_estimates counts results from deep descendants (grandchildren)
- `test_check_estimates_filters_by_type` — check_estimates only counts results of expected types

### `core/test_verify_ssl.py`
- `test_verify_false_round_trip` — verify=False round-trips through insert → dequeue → deserialize
- `test_verify_true_round_trip` — Default verify (None in DB) deserializes to True
- `test_verify_ca_bundle_round_trip` — CA bundle path string round-trips correctly
- `test_serialize_deserialize_verify_false` — _serialize_request produces verify='false' for verify=False
- `test_serialize_deserialize_verify_true` — _serialize_request produces verify=None for verify=True
- `test_verify_false_survives_resolve_from` — verify=False survives resolve_from (the enqueue_request path)
- `test_verify_false_end_to_end` — Full end-to-end: insert verify=False, dequeue, fetch via AsyncRequestManager

### `core/test_serialization.py`
- `test_navigating_request_round_trip` — Navigating Request serializes and deserializes through DB correctly
- `test_non_navigating_request_round_trip` — Non-navigating Request round-trips through DB with binary body preserved
- `test_archive_request_round_trip` — Archive Request round-trips through DB with expected_type preserved
- `test_archive_request_without_expected_type` — Archive Request round-trips when expected_type is None
- `test_request_with_binary_body` — Request with binary body data round-trips correctly
- `test_request_with_empty_optional_fields` — Minimal request with empty optionals round-trips with correct defaults
- `test_bypass_rate_limit_round_trip` — bypass_rate_limit=True round-trips through DB correctly
- `test_bypass_rate_limit_default_false` — bypass_rate_limit defaults to False when not set

### `core/test_infrastructure.py`
- `test_basic_compress_decompress` — Basic compress/decompress roundtrip
- `test_compression_ratio` — Compression achieves good ratios on repetitive content
- `test_compress_response_no_dict` — compress_response works without dictionary
- `test_queue_stats` — Queue statistics counts by status and continuation
- `test_compression_stats` — Compression statistics calculates ratios and totals
- `test_stats_json_serialization` — Stats serialize to JSON with all sections
- `test_train_compression_dict` — Trains compression dictionary from stored responses
- `test_recompress_responses` — Recompresses responses with trained dictionary for better ratios
- `test_train_dict_no_responses_raises` — Training with no responses raises ValueError
- `test_recompress_no_dict_raises` — Recompressing without a dict raises ValueError
- `test_response_compression_roundtrip` — Full driver round-trip: response is compressed and decompressed correctly
- `test_put_and_count` — AioSQLiteBucket: add items and count by weight
- `test_peek` — AioSQLiteBucket: peek at items by index (newest first)
- `test_leak` — AioSQLiteBucket: leak expired items by timestamp
- `test_flush` — AioSQLiteBucket: flush all items
- `test_waiting` — AioSQLiteBucket: calculate wait time when at capacity
- `test_waiting_no_wait_needed` — AioSQLiteBucket: no wait when under limit
- `test_rate_limiter_created_from_scraper_rates` — Driver creates rate limiter from scraper.rate_limits

### `core/test_shutdown.py`
- `test_shutdown_resets_in_progress_to_pending` — Closing driver resets in_progress requests to pending
- `test_resume_restores_pending_requests` — resume=True restores in_progress requests to pending on open
- `test_full_shutdown_and_resume_cycle` — Complete shutdown and resume cycle preserves all requests
- `test_stop_event_signals_workers_to_stop` — Setting stop_event causes workers to exit gracefully
- `test_run_metadata_status_transitions` — Run metadata status updated correctly during lifecycle
- `test_no_data_loss_on_shutdown` — No requests lost during shutdown cycle
- `test_status_method_reflects_queue_state` — status() correctly reflects queue state (unstarted/in_progress/done)
- `test_get_next_request_returns_pending_only` — _get_next_request only returns pending requests
- `test_held_requests_not_returned` — Held requests are skipped by _get_next_request
- `test_pause_and_resume_step` — pause_step and resume_step hold/release requests by continuation
- `test_stop_event_stops_workers` — Setting stop_event causes workers to exit gracefully with interrupted status
- `test_signal_handler_setup_and_teardown` — Signal handlers are set up and torn down properly
- `test_resume_after_interrupt` — Interrupted requests can be resumed on next run

### `core/test_deduplication.py`
- `test_duplicate_requests_are_skipped` — Requests with the same deduplication_key are skipped
- `test_different_urls_are_not_deduplicated` — Requests to different URLs are not deduplicated
- `test_cycle_prevention_via_deduplication` — Deduplication prevents cycles (A -> B -> A)
- `test_custom_deduplication_key` — Custom deduplication keys work correctly
- `test_skip_deduplication_check` — SkipDeduplicationCheck allows duplicate requests
- `test_dedup_with_post_data` — POST requests with same URL but different body are not deduplicated
- `test_dedup_with_same_post_data` — POST requests with same URL and same body are deduplicated
- `test_child_requests_track_parent` — Child requests properly track their parent request ID
- `test_archive_request_tracks_parent` — Archive requests properly track their parent request ID

### `core/test_errors.py`
- `test_store_and_retrieve_error` — Store and retrieve an error record
- `test_resolve_error` — Resolve an error with notes
- `test_list_errors_filter` — List errors with type and resolution filters
- `test_exponential_backoff_calculation` — Retry delays follow exponential backoff with cap
- `test_retry_respects_max_backoff` — Cumulative backoff capped at max_backoff_time
- `test_transient_error_triggers_retry` — Transient errors trigger retry with backoff via PersistentDriver
- `test_max_backoff_exceeded_marks_failed` — Exceeding max backoff marks request as failed

### `core/test_listing.py`
- `test_list_requests` — List requests with status/continuation filters and pagination
- `test_list_responses` — List responses with continuation filter
- `test_record_to_json` — RequestRecord, ResponseRecord, ResultRecord, Page serialize to JSON
- `test_cancel_request` — Cancel a pending request
- `test_cancel_requests_by_continuation` — Batch cancel requests by continuation name
- `test_list_requests_by_status` — Driver list_requests filters by status (pending/completed/failed/held/in_progress)
- `test_list_requests_by_continuation` — Driver list_requests filters by continuation
- `test_list_requests_pagination` — Driver list_requests handles pagination with limit/offset
- `test_list_responses_filtering` — Driver list_responses filters by continuation
- `test_list_responses_pagination` — Driver list_responses handles pagination
- `test_list_results_filtering` — Driver list_results filters by result_type and is_valid

### `sql_manager/test_base.py`
- `test_open_context_manager` — SQLManager.open context manager creates and closes properly
- `test_engine_property` — Engine is set on the manager after opening

### `sql_manager/test_types.py`
- `test_request_record_to_dict` — RequestRecord.to_dict() and to_json() serialize correctly
- `test_response_record_to_dict` — ResponseRecord.to_dict() includes compression_ratio
- `test_result_record_to_dict` — ResultRecord.to_dict() parses JSON fields
- `test_page_to_dict` — Page.to_dict() and to_json() serialize with pagination metadata

### `sql_manager/test_listing.py`
- `test_list_requests_by_status` — List requests filtered by pending/completed status
- `test_list_requests_by_continuation` — List requests filtered by continuation name
- `test_list_requests_pagination` — Pagination with limit/offset/has_more on list_requests
- `test_list_responses` — List responses with continuation filter
- `test_list_results` — List results filtered by type and validity
- `test_get_request_found` — get_request returns matching request by ID
- `test_get_request_not_found` — get_request returns None for missing ID
- `test_get_response_found` — get_response returns matching response by ID
- `test_get_response_not_found` — get_response returns None for missing ID
- `test_get_result_found` — get_result returns matching result by ID with parsed JSON
- `test_get_result_not_found` — get_result returns None for missing ID
- `test_get_run_status_unstarted` — Run status is "unstarted" with no requests
- `test_get_run_status_in_progress` — Run status is "in_progress" with pending requests
- `test_get_run_status_done` — Run status is "done" when all completed

### `sql_manager/test_requests.py`
- `test_insert_request` — Insert a request and verify it in the database
- `test_check_dedup_key_exists` — Dedup key existence check before and after insert
- `test_get_next_pending_request` — Priority queue returns highest priority request first
- `test_mark_request_in_progress` — Marking request sets status and started_at timestamp
- `test_mark_request_completed` — Marking request sets status and completed_at timestamp
- `test_mark_request_failed` — Marking request failed sets status and last_error
- `test_restore_queue` — restore_queue resets in_progress requests to pending
- `test_count_methods` — count_pending, count_active, count_all track request lifecycle
- `test_pause_step` — Pause holds pending requests by continuation
- `test_resume_step` — Resume releases held requests back to pending
- `test_cancel_request` — Cancel sets pending request to failed with "Cancelled" error
- `test_cancel_request_not_pending` — Completed requests cannot be cancelled
- `test_cancel_requests_by_continuation` — Batch cancel by continuation name
- `test_no_completed_requests` — avg_completed_request_duration_s returns None when empty
- `test_completed_requests_with_timestamps` — avg_completed_request_duration_s returns positive float
- `test_sample_size_limits_rows` — sample_size parameter limits averaged rows
- `test_empty_db` — continuations_needing_compression_dict returns empty on no data
- `test_below_threshold` — Continuation below threshold not returned
- `test_at_threshold` — Continuation at threshold is returned
- `test_dict_compressed_not_counted` — Responses with dict_id excluded from threshold count

### `sql_manager/test_estimates.py`
- `test_store_estimate` — Store an estimate and verify fields in the database
- `test_store_estimate_unbounded_max` — Store an estimate with max_count=None

### `sql_manager/test_incidental_requests.py`
- `test_insert_creates_both_rows` — Inserting an incidental request creates both incidental_requests and storage rows
- `test_deduplication_same_content` — Identical content deduplicates to one storage row
- `test_different_content_no_dedup` — Different content creates separate storage rows
- `test_no_content_creates_storage_row` — Request with no content still creates a storage row
- `test_dedup_across_parents` — Content deduplication works across different parent requests
- `test_get_incidental_requests` — Retrieve all incidental requests for a parent, ordered by started_at_ns
- `test_get_incidental_request_by_id` — Retrieve a single incidental request by ID
- `test_get_incidental_request_not_found` — Returns None for nonexistent incidental request ID
- `test_get_incidental_request_storage` — Retrieve storage row with compressed content and headers
- `test_duration_ms` — IncidentalRequestRecord computes duration_ns and duration_ms from timestamps
- `test_duration_ms_no_timing` — IncidentalRequestRecord returns None duration when timestamps absent
- `test_compression_ratio` — IncidentalRequestRecord computes compression ratio from sizes
- `test_to_dict` — IncidentalRequestRecord.to_dict() includes computed fields
- `test_to_json` — IncidentalRequestRecord.to_json() produces valid JSON


### `sql_manager/test_responses.py`
- `test_store_response` — Store an HTTP response and verify status code and content size
- `test_get_response_content` — Retrieve decompressed response content
- `test_get_response_content_empty` — Retrieve empty response content for headers-only responses

### `sql_manager/test_results.py`
- `test_store_result_valid` — Store a valid result and verify type, validity flag, and JSON data
- `test_store_result_invalid` — Store an invalid result with validation errors and verify flags

### `sql_manager/test_run_metadata.py`
- `test_init_run_metadata_new` — Initialize new run metadata and verify scraper name and worker count
- `test_init_run_metadata_idempotent` — Calling init_run_metadata twice does not create duplicate rows
- `test_update_run_status` — Update run status field in metadata

### `sql_manager/test_validation.py`
- `test_validate_json_responses_all_valid` — Validation returns empty list when all responses match the model
- `test_validate_json_responses_some_invalid` — Validation returns IDs of responses missing required fields
- `test_validate_json_responses_no_responses` — Validation returns empty list for nonexistent continuation
- `test_validate_json_responses_malformed_json` — Validation catches malformed JSON content
- `test_validate_json_responses_empty_content` — Validation skips responses with empty/null content

### `web/test_app.py`
- `test_scan_runs_empty_dir` — Scanning empty runs directory returns empty list
- `test_scan_runs_creates_missing_dir` — scan_runs creates directory if it doesn't exist
- `test_scan_runs_discovers_databases` — Scanning discovers .db files and ignores non-db files
- `test_list_runs` — Listing all runs returns correct run IDs
- `test_get_run_found` — Getting an existing run returns its info
- `test_get_run_not_found` — Getting a non-existent run returns None
- `test_create_run` — Creating a new run initializes driver and database
- `test_create_run_duplicate_raises` — Creating a duplicate run raises ValueError
- `test_load_run` — Loading an existing unloaded run sets status to loaded
- `test_load_run_not_found` — Loading a non-existent run raises ValueError
- `test_unload_run` — Unloading a loaded run sets status to unloaded and clears driver
- `test_delete_run` — Deleting a run removes it from manager and deletes database file
- `test_delete_run_running_raises` — Deleting a running run raises ValueError
- `test_run_info_to_dict` — RunInfo serialization includes all fields
- `test_shutdown_all_empty` — shutdown_all with no runs does not raise
- `test_shutdown_all_unloads_all` — shutdown_all closes all driver connections
- `test_create_app` — Creating FastAPI app with custom runs directory
- `test_create_app_default_dir` — Creating FastAPI app uses default runs directory
- `test_get_run_manager_not_initialized` — get_run_manager raises when not initialized
- `test_lifespan_initializes_manager` — Lifespan initializes and cleans up run manager
- `test_list_runs_empty` — GET /api/runs returns empty list when no runs
- `test_list_runs_with_databases` — GET /api/runs returns existing database runs
- `test_get_run_not_found` — GET /api/runs/:id returns 404 for missing run
- `test_get_run_found` — GET /api/runs/:id returns existing run info
- `test_delete_run_not_found` — DELETE /api/runs/:id returns 404 for missing run
- `test_delete_run_success` — DELETE /api/runs/:id removes run and database
- `test_scan_runs` — POST /api/runs/scan discovers newly added databases
- `test_start_run_not_found` — POST /api/runs/:id/start returns 404 for missing run
- `test_start_run_not_loaded` — POST /api/runs/:id/start returns 400 for unloaded run
- `test_stop_run_not_found` — POST /api/runs/:id/stop returns 404 for missing run
- `test_stop_run_not_running` — POST /api/runs/:id/stop returns 400 for non-running run
- `test_unload_run_not_found` — POST /api/runs/:id/unload returns 404 for missing run
- `test_create_run_scraper_not_found` — POST /api/runs with unknown scraper returns 404
- `test_connect_and_disconnect` — WebSocket connect and disconnect updates connection counts
- `test_broadcast` — Broadcasting events sends to all subscribed WebSockets
- `test_subscription_filtering` — Events are filtered by subscription event types
- `test_update_subscription` — Updating subscription changes event filter set
- `test_progress_event_types` — ProgressEventType enum has correct string values
- `test_create_progress_callback` — Progress callback broadcasts events to connected WebSockets

### `web/test_routes.py`
- `test_summary_empty_db` — Request summary returns empty list for no requests
- `test_summary_endpoint_grouping` — Request summary counts correctly per continuation/status
- `test_stats_by_continuation_empty_db` — Compression stats returns empty list for no responses
- `test_stats_by_continuation_grouping` — Compression stats groups by continuation with correct ratios
- `test_results_summary_empty_db` — Results summary returns zeros for no results
- `test_results_summary_counts_by_type` — Results summary counts valid/invalid by result type
- `test_jsonl_export_format` — JSONL export lines are valid JSON with correct fields
- `test_jsonl_export_with_filter` — JSONL export respects result_type and is_valid filters

### `core/test_driver_api.py`
- `test_same_results_as_async_driver` — PersistentDriver produces same results as AsyncDriver
- `test_persistent_driver_persists_results` — PersistentDriver stores results, requests, and responses in database
- `test_get_response_found` — get_response returns response when found
- `test_get_response_not_found` — get_response returns None when not found
- `test_get_result_found` — get_result returns result when found
- `test_get_result_not_found` — get_result returns None when not found
- `test_cancel_request_pending` — Cancelling a pending request marks it failed
- `test_cancel_request_held` — Cancelling a held request marks it failed
- `test_cancel_request_in_progress_fails` — Cancelling an in_progress request returns False
- `test_cancel_request_not_found` — Cancelling a non-existent request returns False
- `test_cancel_requests_by_continuation` — Cancels all pending/held requests for a continuation (not in_progress)
- `test_cancel_requests_by_continuation_empty` — Cancelling by non-existent continuation returns 0

### `core/test_request_processing.py`
- `test_successful_request_marked_completed` — Successful requests are marked completed with completed_at timestamp
- `test_failed_request_marked_failed` — Requests with structural errors are marked failed with last_error
- `test_parsed_data_stored_in_results` — ParsedData is correctly stored in results table with type and JSON
- `test_headers_only_response_storage` — Responses with no body store headers and empty content correctly
- `test_valid_deferred_validation_stored_and_callback_called` — Valid DeferredValidation data stored as valid and on_data called
- `test_invalid_deferred_validation_stored_as_invalid` — Invalid DeferredValidation stored with is_valid=False and errors
- `test_non_navigating_request_processed` — Non-navigating requests processed and tracked as non_navigating type
- `test_non_navigating_request_preserves_accumulated_data` — Non-navigating requests preserve accumulated_data from parent

### `core/test_scoped_session.py`
- `test_default_scope_is_none` — Default scope is None
- `test_set_and_get_scope` — set_scope / get_scope round-trips correctly
- `test_clear_scope` — clear_scope resets scope to None
- `test_unscoped_returns_different_sessions` — Unscoped calls return different sessions
- `test_unscoped_session_not_cached_in_registry` — Unscoped sessions are not stored in the registry
- `test_scoped_returns_same_session` — Scoped calls return the same cached session
- `test_scoped_session_not_closed_on_exit` — Scoped session remains active after exiting context
- `test_scoped_session_persists_across_operations` — Multiple operations in the same scope reuse the same session
- `test_different_scopes_get_different_sessions` — Different scope keys get different sessions
- `test_contextvar_isolation_across_tasks` — Different asyncio tasks with different scopes get different sessions
- `test_rollback_on_exception_keeps_session_alive` — Scoped session is rolled back (not closed) on exception
- `test_remove_clears_registry` — remove() clears scope key from registry
- `test_remove_nonexistent_key_is_noop` — remove() on nonexistent key does not raise
- `test_remove_all_clears_registry` — remove_all() clears all scoped sessions
- `test_new_session_after_remove` — After remove, a new session is created for that scope
- `test_committed_data_persists_across_scoped_operations` — Data committed in one scoped block is visible in the next

### `core/test_seed_params.py`
- `test_non_spec_entries_run` — seed_params=None runs all non-speculative entries, no speculation
- `test_only_selected_entries_run` — seed_params with one non-speculative entry runs only that entry
- `test_both_non_spec_entries` — seed_params with both non-speculative entries runs both
- `test_speculation_runs_without_non_spec` — seed_params with only speculative entry runs speculation, skips non-speculative
- `test_both_types_run` — seed_params with both speculative and non-speculative entries runs both
- `test_empty_list_runs_nothing` — seed_params=[] runs no entries and no speculation

### `migration/test_incidental_storage.py`
- `test_fresh_db_has_both_tables` — Fresh database has incidental_requests and incidental_request_storage tables
- `test_migration_creates_storage_table` — Migrating from v15 creates storage table and adds storage_id column
- `test_backfill_and_dedup` — Migration backfills storage rows and deduplicates identical content
- `test_migration_idempotent` — Running migration twice produces the same result

### `cli/test_analysis.py`
- `test_diagnose_error_without_response` — Diagnose command fails when error has no response
- `test_diagnose_error_not_found` — Diagnose command fails for non-existent error ID
- `test_export_jsonl` — Results export produces valid JSONL with expected fields
- `test_export_jsonl_filtered` — Results export with --valid filter returns only valid results

### `cli/test_doctor.py`
- `test_scrape_health_table_format` — Scrape health outputs table with status, integrity, errors, ghosts sections
- `test_scrape_health_json_format` — Scrape health JSON includes status, integrity, ghosts, error_stats
- `test_scrape_health_jsonl_format` — Scrape health JSONL includes status, integrity, ghosts, error_stats, estimates
- `test_scrape_db_on_group` — --db flag on scrape group propagates to subcommands
- `test_requests_orphans_table_format` — Requests orphans outputs table with orphaned requests and responses
- `test_requests_orphans_json_format` — Requests orphans JSON has orphaned_requests and orphaned_responses lists
- `test_requests_orphans_jsonl_format` — Requests orphans JSONL output is valid
- `test_requests_pending_table_format` — Requests pending outputs table with total pending count
- `test_requests_pending_json_format` — Requests pending JSON has total and items fields
- `test_requests_pending_with_limit` — Requests pending respects --limit option
- `test_requests_ghosts_table_format` — Requests ghosts outputs table with total count
- `test_requests_ghosts_json_format` — Requests ghosts JSON has total_count, by_continuation, items
- `test_requests_ghosts_jsonl_format` — Requests ghosts JSONL output is valid
- `test_requests_ghosts_with_step_filter` — Requests ghosts respects --step filter
- `test_requests_ghosts_nonexistent_step` — Requests ghosts with nonexistent step shows "No ghost requests found"
- `test_scrape_health_includes_estimates` — Scrape health table includes Estimates section
- `test_scrape_health_json_includes_estimates` — Scrape health JSON includes estimates field
- `test_scrape_estimates_table_format` — Scrape estimates outputs table with Estimate Checks
- `test_scrape_estimates_json_format` — Scrape estimates JSON has items and summary
- `test_scrape_estimates_failures_only` — Scrape estimates respects --failures-only flag

### `cli/test_errors.py`
- `test_errors_list` — Lists all errors with total count
- `test_errors_list_filter_by_type` — Filters errors by type (xpath, http)
- `test_errors_list_filter_by_resolution` — Filters errors by unresolved status
- `test_errors_show` — Shows error details by ID
- `test_errors_summary` — Displays error summary with totals and breakdown
- `test_errors_resolve` — Marks an error as resolved with notes
- `test_results_list` — Lists all results with total count
- `test_results_list_filter_by_validity` — Filters results by valid/invalid
- `test_results_show` — Shows result details by ID
- `test_results_summary` — Displays result summary by type

### `cli/test_info.py`
- `test_info_table_format` — Info command outputs table with metadata and statistics
- `test_info_json_format` — Info command outputs valid JSON with metadata and stats
- `test_info_nonexistent_db` — Info command fails for non-existent database
- `test_table_format_default` — Table format is the default output format
- `test_json_format` — JSON output format produces valid JSON
- `test_jsonl_format` — JSONL output format produces valid newline-delimited JSON
- `test_invalid_database_path` — Commands fail with invalid database path
- `test_invalid_format_option` — Commands fail with invalid format option
- `test_missing_required_argument` — Commands fail when required arguments missing
- `test_invalid_request_id` — Commands fail with non-numeric request ID

### `cli/test_operations.py`
- `test_cancel_request` — Cancel a pending request by ID
- `test_cancel_request_not_pending` — Cancel fails for non-pending request
- `test_cancel_continuation` — Cancel all pending requests for a continuation step
- `test_compression_stats` — Compression stats command outputs statistics
- `test_compression_stats_json_format` — Compression stats in JSON format produces valid JSON
- `test_compression_train` — Compression train command for a continuation step
- `test_compression_recompress` — Compression recompress command for a continuation step

### `cli/test_requests.py`
- `test_requests_list` — List all requests with total count
- `test_requests_list_filter_by_status` — Filter request list by status
- `test_requests_list_filter_by_step` — Filter request list by step name
- `test_requests_list_json_format` — Request list in JSON format with items and total
- `test_requests_list_pagination` — Request list with limit/offset pagination
- `test_requests_show` — Show single request details by ID
- `test_requests_show_json_format` — Show request in JSON format with all fields
- `test_requests_show_not_found` — Show fails for non-existent request ID
- `test_requests_summary` — Request summary grouped by step
- `test_requests_content` — Display response content for a request
- `test_requests_content_to_file` — Export response content to file

### `cli/test_integration.py`
- `test_workflow_inspect_error_and_resolve` — Workflow: show error details, resolve it, verify unresolved count drops
- `test_workflow_export_results` — Workflow: check results summary, export valid results to JSONL, verify file contents
