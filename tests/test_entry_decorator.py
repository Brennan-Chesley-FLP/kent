"""Tests for the @entry decorator and related functionality.

This test module verifies:
- The @entry decorator attaches EntryMetadata correctly
- is_entry() and get_entry_metadata() helpers work
- BaseScraper.list_entries() discovers decorated methods
- EntryMetadata.validate_params() handles BaseModel and primitive types
- BaseScraper.initial_seed() dispatches correctly
- BaseScraper.schema() generates correct JSON Schema
- Speculative entries work through @entry(speculative=SimpleSpeculation(...))
- Error cases (empty params, unknown entry, bad types)
"""

from collections.abc import Generator
from datetime import date

import pytest
from pydantic import BaseModel

from kent.common.decorators import (
    entry,
    get_entry_metadata,
    is_entry,
)
from kent.common.param_models import DateRange
from kent.common.speculation_types import SimpleSpeculation
from kent.data_types import (
    BaseScraper,
    EntryInfo,
    HttpMethod,
    HTTPRequestParams,
    NavigatingRequest,
)

# ── Test fixtures ──────────────────────────────────────────────────


class FakeData(BaseModel):
    name: str


class OpinionFilters(BaseModel):
    court_id: str
    year: int


class SimpleScraper(BaseScraper[FakeData]):
    @entry(FakeData)
    def search_by_name(
        self, name: str
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url=f"/search?name={name}"
            ),
            continuation="parse_results",
        )

    @entry(FakeData)
    def search_by_date(
        self, date_range: DateRange
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET,
                url=f"/search?start={date_range.start}&end={date_range.end}",
            ),
            continuation="parse_results",
        )

    @entry(
        FakeData,
        speculative=SimpleSpeculation(
            highest_observed=500,
            largest_observed_gap=20,
        ),
    )
    def fetch_by_id(
        self, record_id: int
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url=f"/record/{record_id}"
            ),
            continuation="parse_detail",
        )


class MultiTypeScraper(BaseScraper[FakeData]):
    @entry(FakeData)
    def search_opinions(
        self, filters: OpinionFilters
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/opinions"),
            continuation="parse_opinions",
        )

    @entry(FakeData)
    def search_by_filing_date(
        self, filing_date: date
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(method=HttpMethod.GET, url="/filings"),
            continuation="parse_filings",
        )

    @entry(FakeData)
    def search_by_count(
        self, count: int
    ) -> Generator[NavigatingRequest, None, None]:
        yield NavigatingRequest(
            request=HTTPRequestParams(
                method=HttpMethod.GET, url=f"/search?count={count}"
            ),
            continuation="parse_results",
        )


# ── @entry metadata attachment ─────────────────────────────────────


class TestEntryMetadata:
    def test_entry_attaches_metadata(self):
        meta = get_entry_metadata(SimpleScraper.search_by_name)
        assert meta is not None
        assert meta.return_type is FakeData
        assert meta.func_name == "search_by_name"
        assert meta.param_types == {"name": str}
        assert meta.speculative is False

    def test_entry_with_basemodel_param(self):
        meta = get_entry_metadata(SimpleScraper.search_by_date)
        assert meta is not None
        assert meta.param_types == {"date_range": DateRange}

    def test_speculative_entry_metadata(self):
        meta = get_entry_metadata(SimpleScraper.fetch_by_id)
        assert meta is not None
        assert meta.speculative is True
        assert isinstance(meta.speculation, SimpleSpeculation)
        assert meta.speculation.highest_observed == 500
        assert meta.speculation.largest_observed_gap == 20
        assert meta.param_types == {"record_id": int}

    def test_entry_metadata_is_frozen(self):
        meta = get_entry_metadata(SimpleScraper.search_by_name)
        with pytest.raises(AttributeError):
            meta.func_name = "something_else"  # type: ignore[misc]

    def test_date_param_type(self):
        meta = get_entry_metadata(MultiTypeScraper.search_by_filing_date)
        assert meta is not None
        assert meta.param_types == {"filing_date": date}

    def test_complex_basemodel_param(self):
        meta = get_entry_metadata(MultiTypeScraper.search_opinions)
        assert meta is not None
        assert meta.param_types == {"filters": OpinionFilters}


# ── is_entry / get_entry_metadata helpers ──────────────────────────


class TestEntryHelpers:
    def test_is_entry_true(self):
        assert is_entry(SimpleScraper.search_by_name)

    def test_is_entry_false_for_non_entry(self):
        assert not is_entry(SimpleScraper.fails_successfully)

    def test_get_entry_metadata_returns_none_for_non_entry(self):
        assert get_entry_metadata(SimpleScraper.fails_successfully) is None


# ── list_entries() ─────────────────────────────────────────────────


class TestListEntries:
    def test_discovers_all_entries(self):
        entries = SimpleScraper.list_entries()
        names = {e.name for e in entries}
        assert names == {"search_by_name", "search_by_date", "fetch_by_id"}

    def test_entry_info_fields(self):
        entries = SimpleScraper.list_entries()
        by_name = {e.name: e for e in entries}

        info = by_name["search_by_name"]
        assert isinstance(info, EntryInfo)
        assert info.return_type is FakeData
        assert info.param_types == {"name": str}
        assert info.speculative is False

    def test_speculative_entry_in_list(self):
        entries = SimpleScraper.list_entries()
        by_name = {e.name: e for e in entries}

        info = by_name["fetch_by_id"]
        assert info.speculative is True
        assert isinstance(info.speculation, SimpleSpeculation)
        assert info.speculation.highest_observed == 500
        assert info.speculation.largest_observed_gap == 20


# ── validate_params() ─────────────────────────────────────────────


class TestValidateParams:
    def test_validate_primitive_str(self):
        meta = get_entry_metadata(SimpleScraper.search_by_name)
        result = meta.validate_params({"name": "alice"})
        assert result == {"name": "alice"}

    def test_validate_primitive_int(self):
        meta = get_entry_metadata(MultiTypeScraper.search_by_count)
        result = meta.validate_params({"count": 42})
        assert result == {"count": 42}

    def test_validate_speculative_range(self):
        meta = get_entry_metadata(SimpleScraper.fetch_by_id)
        result = meta.validate_params({"record_id": [1, 42]})
        assert result == {"record_id": (1, 42)}

    def test_validate_basemodel(self):
        meta = get_entry_metadata(SimpleScraper.search_by_date)
        result = meta.validate_params(
            {"date_range": {"start": "2020-01-01", "end": "2020-12-31"}}
        )
        assert isinstance(result["date_range"], DateRange)
        assert result["date_range"].start == date(2020, 1, 1)
        assert result["date_range"].end == date(2020, 12, 31)

    def test_validate_date_from_string(self):
        meta = get_entry_metadata(MultiTypeScraper.search_by_filing_date)
        result = meta.validate_params({"filing_date": "2024-06-15"})
        assert result["filing_date"] == date(2024, 6, 15)

    def test_validate_date_from_date_object(self):
        meta = get_entry_metadata(MultiTypeScraper.search_by_filing_date)
        result = meta.validate_params({"filing_date": date(2024, 6, 15)})
        assert result["filing_date"] == date(2024, 6, 15)

    def test_validate_missing_param_raises(self):
        meta = get_entry_metadata(SimpleScraper.search_by_name)
        with pytest.raises(ValueError, match="Missing required parameter"):
            meta.validate_params({})

    def test_validate_unexpected_param_raises(self):
        meta = get_entry_metadata(SimpleScraper.search_by_name)
        with pytest.raises(ValueError, match="Unexpected parameters"):
            meta.validate_params({"name": "alice", "extra": "bad"})

    def test_validate_bad_date_raises(self):
        meta = get_entry_metadata(MultiTypeScraper.search_by_filing_date)
        with pytest.raises(TypeError, match="expected date or ISO string"):
            meta.validate_params({"filing_date": 12345})


# ── initial_seed() ─────────────────────────────────────────────────


class TestInitialSeed:
    def test_single_invocation(self):
        scraper = SimpleScraper()
        requests = list(
            scraper.initial_seed([{"search_by_name": {"name": "test"}}])
        )
        assert len(requests) == 1
        assert requests[0].request.url == "/search?name=test"

    def test_multiple_invocations(self):
        scraper = SimpleScraper()
        requests = list(
            scraper.initial_seed(
                [
                    {"search_by_name": {"name": "alice"}},
                    {"search_by_name": {"name": "bob"}},
                ]
            )
        )
        assert len(requests) == 2
        assert requests[0].request.url == "/search?name=alice"
        assert requests[1].request.url == "/search?name=bob"

    def test_speculative_initial_seed_stores_overrides(self):
        scraper = SimpleScraper()
        requests = list(
            scraper.initial_seed(
                [{"fetch_by_id": {"record_id": [1, 99]}}]
            )
        )
        # Speculative entries don't yield requests
        assert len(requests) == 0
        # Instead they store overrides
        assert hasattr(scraper, "_speculation_overrides")
        assert "fetch_by_id" in scraper._speculation_overrides
        assert scraper._speculation_overrides["fetch_by_id"] == [
            {"record_id": (1, 99)}
        ]

    def test_basemodel_param_dispatch(self):
        scraper = SimpleScraper()
        requests = list(
            scraper.initial_seed(
                [
                    {
                        "search_by_date": {
                            "date_range": {
                                "start": "2020-01-01",
                                "end": "2020-12-31",
                            }
                        }
                    }
                ]
            )
        )
        assert len(requests) == 1
        assert "start=2020-01-01" in requests[0].request.url

    def test_empty_params_raises(self):
        scraper = SimpleScraper()
        with pytest.raises(ValueError, match="at least one parameter"):
            list(scraper.initial_seed([]))

    def test_none_params_raises(self):
        scraper = SimpleScraper()
        with pytest.raises((ValueError, TypeError)):
            list(scraper.initial_seed(None))  # type: ignore[arg-type]

    def test_unknown_entry_raises(self):
        scraper = SimpleScraper()
        with pytest.raises(ValueError, match="Unknown entry"):
            list(scraper.initial_seed([{"nonexistent": {}}]))


# ── schema() ───────────────────────────────────────────────────────


class TestSchema:
    def test_schema_structure(self):
        schema = SimpleScraper.schema()
        assert schema["scraper"] == "SimpleScraper"
        assert "entries" in schema
        assert set(schema["entries"].keys()) == {
            "search_by_name",
            "search_by_date",
            "fetch_by_id",
        }

    def test_primitive_param_schema(self):
        schema = SimpleScraper.schema()
        entry = schema["entries"]["search_by_name"]
        assert entry["returns"] == "FakeData"
        assert entry["speculative"] is False
        props = entry["parameters"]["properties"]
        assert props["name"] == {"type": "string"}
        assert entry["parameters"]["required"] == ["name"]

    def test_basemodel_param_schema(self):
        schema = SimpleScraper.schema()
        entry = schema["entries"]["search_by_date"]
        props = entry["parameters"]["properties"]
        assert props["date_range"] == {"$ref": "#/$defs/DateRange"}
        assert "DateRange" in schema["$defs"]

    def test_speculative_schema(self):
        schema = SimpleScraper.schema()
        entry = schema["entries"]["fetch_by_id"]
        assert entry["speculative"] is True
        assert entry["highest_observed"] == 500
        assert entry["largest_observed_gap"] == 20
        # Speculative entries use range schema
        props = entry["parameters"]["properties"]
        assert props["record_id"] == {
            "type": "array",
            "items": {"type": "integer"},
            "minItems": 2,
            "maxItems": 2,
        }

    def test_integer_param_schema(self):
        schema = MultiTypeScraper.schema()
        entry = schema["entries"]["search_by_count"]
        props = entry["parameters"]["properties"]
        assert props["count"] == {"type": "integer"}

    def test_date_param_schema(self):
        schema = MultiTypeScraper.schema()
        entry = schema["entries"]["search_by_filing_date"]
        props = entry["parameters"]["properties"]
        assert props["filing_date"] == {"type": "string", "format": "date"}

    def test_schema_is_json_serializable(self):
        import json

        schema = SimpleScraper.schema()
        # Should not raise
        json.dumps(schema)


# ── Decorator error cases ──────────────────────────────────────────


class TestEntryDecoratorErrors:
    def test_tuple_param_rejected(self):
        with pytest.raises(TypeError, match="tuple type.*not supported"):

            class BadScraper(BaseScraper[FakeData]):
                @entry(FakeData)
                def bad_entry(
                    self, pair: tuple
                ) -> Generator[NavigatingRequest, None, None]:
                    yield NavigatingRequest(
                        request=HTTPRequestParams(
                            method=HttpMethod.GET, url="/"
                        ),
                        continuation="x",
                    )

    def test_unannotated_param_rejected(self):
        with pytest.raises(TypeError, match="must have a type annotation"):

            @entry(FakeData)
            def bad(self, x):
                pass

    def test_unsupported_type_rejected(self):
        with pytest.raises(TypeError, match="unsupported type"):

            @entry(FakeData)
            def bad(self, x: list) -> Generator[NavigatingRequest, None, None]:  # type: ignore[empty-body]
                ...
