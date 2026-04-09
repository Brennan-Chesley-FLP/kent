"""Tests for Step 19: Step Decorators with Argument Inspection.

This module tests the @step decorator that uses argument inspection to
automatically inject values into scraper methods based on parameter names.

Key behaviors tested:
- Automatic injection of response, request, previous_request
- Automatic injection of json_content, lxml_tree, text
- Auto-resolution of Callable continuations to string names
- Priority metadata attachment
- Encoding parameter support
- Error handling for parsing failures
"""

from collections.abc import Generator

from kent.common.decorators import (
    get_step_metadata,
    is_step,
    step,
)
from kent.common.exceptions import (
    ScraperAssumptionException,
)
from kent.data_types import (
    BaseScraper,
    HttpMethod,
    HTTPRequestParams,
    ParsedData,
    Request,
    Response,
    ScraperYield,
)
from kent.driver.sync_driver import SyncDriver
from tests.utils import collect_results


class TestResponseInjection:
    """Tests for response parameter injection."""

    def test_response_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject response when parameter is named 'response'."""

        class ResponseScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Response should be injected
                assert response is not None
                assert response.url == f"{server_url}/test"
                yield ParsedData(data={"url": response.url})

        scraper = ResponseScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["url"] == f"{server_url}/test"


class TestRequestInjection:
    """Tests for request parameter injection."""

    def test_request_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject request when parameter is named 'request'."""

        class RequestScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self, request, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Request should be injected
                assert request is not None
                assert request.request.url == f"{server_url}/test"
                yield ParsedData(data={"url": request.request.url})

        scraper = RequestScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["url"] == f"{server_url}/test"


class TestPreviousRequestInjection:
    """Tests for previous_request parameter injection."""

    def test_previous_request_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject previous_request from the request chain."""

        class PreviousScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_first",
                )

            @step
            def parse_first(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Navigate to second page
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page2",
                    ),
                    continuation="parse_second",
                )

            @step
            def parse_second(
                self, previous_request, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Previous request should be injected
                assert previous_request is not None
                assert previous_request.request.url == f"{server_url}/test"
                yield ParsedData(
                    data={"previous_url": previous_request.request.url}
                )

        scraper = PreviousScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["previous_url"] == f"{server_url}/test"

    def test_previous_request_none_for_entry(self, server_url: str, tmp_path):
        """The @step decorator shall inject None for previous_request when no previous request exists."""

        class NoPreviousScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self, previous_request, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # No previous request for entry
                assert previous_request is None
                yield ParsedData(data={"has_previous": False})

        scraper = NoPreviousScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["has_previous"] is False


class TestJsonContentInjection:
    """Tests for json_content parameter injection."""

    def test_json_content_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject json_content when parameter is named 'json_content'."""

        class JsonScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/api/cases/BCC-2024-001",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self, json_content: dict
            ) -> Generator[ScraperYield, None, None]:
                # JSON should be parsed and injected
                assert isinstance(json_content, dict)
                assert "docket" in json_content
                yield ParsedData(data=json_content)

        scraper = JsonScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert "docket" in results[0]

    def test_json_parsing_failure_raises_exception(
        self, server_url: str, tmp_path
    ):
        """The @step decorator shall raise ScraperAssumptionException when JSON parsing fails."""

        class BadJsonScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",  # Returns HTML, not JSON
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self, json_content: dict
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data=json_content)

        scraper = BadJsonScraper()
        callback, results = collect_results()
        exceptions = []

        def on_structural_error(exception):
            exceptions.append(exception)
            return False  # Don't continue after error

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
            on_structural_error=on_structural_error,
        )

        driver.run()

        # Should have raised ScraperAssumptionException
        assert len(exceptions) == 1
        assert isinstance(exceptions[0], ScraperAssumptionException)
        assert "Failed to parse JSON" in str(exceptions[0])


class TestLxmlTreeInjection:
    """Tests for lxml_tree parameter injection."""

    def test_lxml_tree_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject lxml_tree when parameter is named 'lxml_tree'."""

        class HtmlScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/cases",
                    ),
                    continuation="parse",
                )

            @step
            def parse(self, lxml_tree) -> Generator[ScraperYield, None, None]:
                # HTML should be parsed and injected as CheckedHtmlElement
                case_rows = lxml_tree.checked_xpath(
                    "//tr[@class='case-row']",
                    "case rows",
                    min_count=1,
                )
                count = len(case_rows)
                yield ParsedData(data={"case_count": count})

        scraper = HtmlScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert "case_count" in results[0]
        assert results[0]["case_count"] > 0


class TestTextInjection:
    """Tests for text parameter injection."""

    def test_text_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject text when parameter is named 'text'."""

        class TextScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(self, text: str) -> Generator[ScraperYield, None, None]:
                # Text should be injected
                assert isinstance(text, str)
                assert len(text) > 0
                yield ParsedData(data={"text_length": len(text)})

        scraper = TextScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["text_length"] > 0


class TestCallableContinuationResolution:
    """Tests for automatic Callable continuation resolution."""

    def test_callable_continuation_resolved_to_string(
        self, server_url: str, tmp_path
    ):
        """The @step decorator shall resolve Callable continuations to function names."""

        class CallableScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_first",
                )

            @step
            def parse_first(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Yield with Callable continuation
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page2",
                    ),
                    continuation=self.parse_second,  # Callable!
                )

            @step
            def parse_second(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={"reached": "parse_second"})

        scraper = CallableScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        # Should successfully resolve and call parse_second
        assert len(results) == 1
        assert results[0]["reached"] == "parse_second"


class TestPriorityMetadata:
    """Tests for priority metadata attachment."""

    def test_default_priority_is_nine(self):
        """The @step decorator shall attach default priority of 9 to decorated functions."""

        class PriorityScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = PriorityScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 9

    def test_custom_priority_attached(self):
        """The @step decorator shall attach custom priority when specified."""

        class CustomPriorityScraper(BaseScraper[dict]):
            @step(priority=3)
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = CustomPriorityScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 3

    def test_is_step_returns_true(self):
        """The is_step function shall return True for decorated methods."""

        class StepScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = StepScraper()
        assert is_step(scraper.parse) is True

    def test_is_step_returns_false_for_undecorated(self):
        """The is_step function shall return False for undecorated methods."""

        class NonStepScraper(BaseScraper[dict]):
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = NonStepScraper()
        assert is_step(scraper.parse) is False

    def test_callable_continuation_inherits_target_priority(self):
        """The @step decorator shall attach target method's priority to yielded requests."""
        from kent.common.decorators import (
            _process_yielded_request,
        )

        class PriorityInheritanceScraper(BaseScraper[dict]):
            @step(priority=7)
            def first_step(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                # Yield a request targeting a method with priority=2
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="http://example.com/next",
                    ),
                    continuation=self.second_step,  # Callable with priority=2
                )

            @step(priority=2)
            def second_step(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = PriorityInheritanceScraper()

        # Create a request with Callable continuation
        request = Request(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url="http://example.com/next",
            ),
            continuation=scraper.second_step,  # Target has priority=2
        )

        # Default priority should be 9
        assert request.priority == 9

        # Process the request - should inherit target's priority
        processed = _process_yielded_request(request)

        # Priority should now be 2 (from target), not 7 (from yielding step)
        assert processed.priority == 2
        assert processed.continuation == "second_step"


class TestEncodingParameter:
    """Tests for encoding parameter support."""

    def test_default_encoding_is_utf8(self):
        """The @step decorator shall use utf-8 as default encoding."""

        class EncodingScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = EncodingScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.encoding == "utf-8"

    def test_custom_encoding_attached(self):
        """The @step decorator shall attach custom encoding when specified."""

        class CustomEncodingScraper(BaseScraper[dict]):
            @step(encoding="latin-1")
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = CustomEncodingScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.encoding == "latin-1"


class TestXsdParameter:
    """Tests for xsd parameter support."""

    def test_default_xsd_is_none(self):
        """The @step decorator shall have xsd=None by default."""

        class NoXsdScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = NoXsdScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.xsd is None

    def test_xsd_attached_when_specified(self):
        """The @step decorator shall attach xsd path when specified."""

        class XsdScraper(BaseScraper[dict]):
            @step(xsd="schemas/court_page.xsd")
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = XsdScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.xsd == "schemas/court_page.xsd"

    def test_xsd_with_other_parameters(self):
        """The @step decorator shall support xsd alongside priority and encoding."""

        class CombinedScraper(BaseScraper[dict]):
            @step(priority=3, encoding="latin-1", xsd="schemas/special.xsd")
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = CombinedScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 3
        assert metadata.encoding == "latin-1"
        assert metadata.xsd == "schemas/special.xsd"


class TestJsonModelParameter:
    """Tests for json_model parameter support."""

    def test_default_json_model_is_none(self):
        """The @step decorator shall have json_model=None by default."""

        class NoJsonModelScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = NoJsonModelScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.json_model is None

    def test_json_model_attached_when_specified(self):
        """The @step decorator shall attach json_model path when specified."""

        class JsonModelScraper(BaseScraper[dict]):
            @step(json_model="api.publications.PublicationsResponse")
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = JsonModelScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.json_model == "api.publications.PublicationsResponse"

    def test_json_model_with_other_parameters(self):
        """The @step decorator shall support json_model alongside priority and encoding."""

        class CombinedScraper(BaseScraper[dict]):
            @step(
                priority=3,
                encoding="utf-8",
                json_model="api.cases.CasesResponse",
            )
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = CombinedScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 3
        assert metadata.encoding == "utf-8"
        assert metadata.json_model == "api.cases.CasesResponse"

    def test_json_model_and_xsd_together(self):
        """The @step decorator shall support both json_model and xsd together."""

        class BothModelsScraper(BaseScraper[dict]):
            @step(
                xsd="schemas/court_page.xsd",
                json_model="api.metadata.MetadataResponse",
            )
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = BothModelsScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.xsd == "schemas/court_page.xsd"
        assert metadata.json_model == "api.metadata.MetadataResponse"


class TestMultipleParameterInjection:
    """Tests for injecting multiple parameters simultaneously."""

    def test_multiple_injections_work_together(
        self, server_url: str, tmp_path
    ):
        """The @step decorator shall support injecting multiple parameters in one function."""

        class MultiInjectionScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self,
                response: Response,
                request,
                lxml_tree,
                text: str,
            ) -> Generator[ScraperYield, None, None]:
                # All should be injected
                assert response is not None
                assert request is not None
                assert lxml_tree is not None
                assert text is not None
                assert isinstance(text, str)

                yield ParsedData(
                    data={
                        "has_response": True,
                        "has_request": True,
                        "has_tree": True,
                        "has_text": True,
                    }
                )

        scraper = MultiInjectionScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["has_response"] is True
        assert results[0]["has_request"] is True
        assert results[0]["has_tree"] is True
        assert results[0]["has_text"] is True


class TestDecoratorSyntax:
    """Tests for @step decorator syntax variations."""

    def test_decorator_without_parens(self):
        """The @step decorator shall work without parentheses."""

        class NoParensScraper(BaseScraper[dict]):
            @step
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = NoParensScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 9

    def test_decorator_with_parens(self):
        """The @step decorator shall work with parentheses and parameters."""

        class ParensScraper(BaseScraper[dict]):
            @step(priority=5, encoding="latin-1")
            def parse(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield ParsedData(data={})

        scraper = ParensScraper()
        metadata = get_step_metadata(scraper.parse)

        assert metadata is not None
        assert metadata.priority == 5
        assert metadata.encoding == "latin-1"


class TestLocalFilepathInjection:
    """Tests for local_filepath parameter injection."""

    def test_local_filepath_injected_for_archive_response(
        self, server_url: str, tmp_path
    ):
        """The @step decorator shall inject local_filepath from ArchiveResponse."""

        class ArchiveScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_entry",
                )

            @step
            def parse_entry(
                self, response: Response
            ) -> Generator[ScraperYield, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/files/example.pdf",
                    ),
                    continuation="archive_file",
                    archive=True,
                )

            @step
            def archive_file(
                self,
                local_filepath: str | None,
                response: Response,
            ) -> Generator[ScraperYield, None, None]:
                # local_filepath should be injected from ArchiveResponse
                assert local_filepath is not None
                assert isinstance(local_filepath, str)
                yield ParsedData(data={"local_filepath": local_filepath})

        scraper = ArchiveScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert "local_filepath" in results[0]
        assert results[0]["local_filepath"] is not None

    def test_local_filepath_none_for_regular_response(
        self, server_url: str, tmp_path
    ):
        """The @step decorator shall inject None for local_filepath when not ArchiveResponse."""

        class RegularScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                )

            @step
            def parse(
                self,
                local_filepath: str | None,
                response: Response,
            ) -> Generator[ScraperYield, None, None]:
                # local_filepath should be None for regular Response
                assert local_filepath is None
                yield ParsedData(data={"local_filepath": local_filepath})

        scraper = RegularScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["local_filepath"] is None


class TestAccumulatedDataInjection:
    """Tests for accumulated_data parameter injection."""

    def test_accumulated_data_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject accumulated_data when parameter is named 'accumulated_data'."""

        class AccumulatedDataScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_first",
                    accumulated_data={"count": 0, "items": []},
                )

            @step
            def parse_first(
                self,
                accumulated_data: dict,
                response: Response,
            ) -> Generator[ScraperYield, None, None]:
                # Modify accumulated_data
                accumulated_data["count"] += 1
                accumulated_data["items"].append("first")
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page2",
                    ),
                    continuation="parse_second",
                    accumulated_data=accumulated_data,
                )

            @step
            def parse_second(
                self,
                accumulated_data: dict,
            ) -> Generator[ScraperYield, None, None]:
                # accumulated_data should have data from previous step
                accumulated_data["count"] += 1
                accumulated_data["items"].append("second")
                yield ParsedData(data=accumulated_data)

        scraper = AccumulatedDataScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["count"] == 2
        assert results[0]["items"] == ["first", "second"]


class TestAuxDataInjection:
    """Tests for aux_data parameter injection."""

    def test_aux_data_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject aux_data when parameter is named 'aux_data'."""

        class AuxDataScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse_first",
                    aux_data={"token": "abc123", "session_id": "xyz"},
                )

            @step
            def parse_first(
                self,
                aux_data: dict,
                response: Response,
            ) -> Generator[ScraperYield, None, None]:
                # aux_data should be injected from request
                assert aux_data["token"] == "abc123"
                assert aux_data["session_id"] == "xyz"
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/page2",
                    ),
                    continuation="parse_second",
                    aux_data=aux_data,
                )

            @step
            def parse_second(
                self,
                aux_data: dict,
            ) -> Generator[ScraperYield, None, None]:
                # aux_data should still have the same values
                yield ParsedData(data={"token": aux_data["token"]})

        scraper = AuxDataScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["token"] == "abc123"


class TestCombinedDataInjection:
    """Tests for combined accumulated_data and aux_data injection."""

    def test_both_data_types_injected(self, server_url: str, tmp_path):
        """The @step decorator shall inject both accumulated_data and aux_data when both are requested."""

        class CombinedDataScraper(BaseScraper[dict]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url=f"{server_url}/test",
                    ),
                    continuation="parse",
                    accumulated_data={"results": []},
                    aux_data={"page_token": "page1"},
                )

            @step
            def parse(
                self,
                accumulated_data: dict,
                aux_data: dict,
                response: Response,
            ) -> Generator[ScraperYield, None, None]:
                # Both should be injected
                accumulated_data["results"].append(aux_data["page_token"])
                yield ParsedData(
                    data={
                        "results": accumulated_data["results"],
                        "token": aux_data["page_token"],
                    }
                )

        scraper = CombinedDataScraper()
        callback, results = collect_results()

        driver = SyncDriver(
            scraper=scraper,
            storage_dir=tmp_path,
            on_data=callback,
        )

        driver.run()

        assert len(results) == 1
        assert results[0]["results"] == ["page1"]
        assert results[0]["token"] == "page1"
