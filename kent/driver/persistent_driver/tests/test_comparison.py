"""Tests for comparison.py - Continuation output comparison logic.

Tests cover:
- Levenshtein distance calculation
- Result pairing via Levenshtein distance
- Dict comparison with field-level diffs
- Request tree comparison (added/removed/modified)
- Data comparison with Levenshtein pairing
- Error comparison (introduced/resolved/changed)
- Summary statistics aggregation
"""

from kent.driver.persistent_driver.comparison import (
    ComparisonResult,
    ComparisonSummary,
    DataDiff,
    ErrorDiff,
    RequestDiff,
    _compare_dicts,
    _compare_errors,
    _levenshtein_distance,
    _pair_results_by_levenshtein,
    compare_continuation_output,
)
from kent.driver.persistent_driver.dry_run_driver import (
    CapturedData,
    CapturedError,
    CapturedRequest,
    DryRunResult,
)


class TestLevenshteinDistance:
    """Test Levenshtein distance calculation."""

    def test_identical_strings(self):
        """Identical strings have distance 0."""
        assert _levenshtein_distance("hello", "hello") == 0
        assert _levenshtein_distance("", "") == 0

    def test_empty_string(self):
        """Distance to empty string is length of other string."""
        assert _levenshtein_distance("hello", "") == 5
        assert _levenshtein_distance("", "world") == 5

    def test_single_substitution(self):
        """Single character substitution has distance 1."""
        assert _levenshtein_distance("hello", "hallo") == 1

    def test_single_insertion(self):
        """Single character insertion has distance 1."""
        assert _levenshtein_distance("hello", "helllo") == 1

    def test_single_deletion(self):
        """Single character deletion has distance 1."""
        assert _levenshtein_distance("hello", "hllo") == 1

    def test_multiple_changes(self):
        """Multiple changes accumulate distance."""
        assert _levenshtein_distance("kitten", "sitting") == 3

    def test_completely_different(self):
        """Completely different strings."""
        assert _levenshtein_distance("abc", "xyz") == 3


class TestPairResultsByLevenshtein:
    """Test Levenshtein-based result pairing."""

    def test_empty_lists(self):
        """Empty lists return empty pairing."""
        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            [], []
        )
        assert paired == []
        assert unpaired_orig == []
        assert unpaired_new == []

    def test_only_original(self):
        """All original results are unpaired when new is empty."""
        original = [
            CapturedData(data={"id": 1}),
            CapturedData(data={"id": 2}),
        ]
        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            original, []
        )
        assert paired == []
        assert len(unpaired_orig) == 2
        assert unpaired_new == []

    def test_only_new(self):
        """All new results are unpaired when original is empty."""
        new = [
            CapturedData(data={"id": 1}),
            CapturedData(data={"id": 2}),
        ]
        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            [], new
        )
        assert paired == []
        assert unpaired_orig == []
        assert len(unpaired_new) == 2

    def test_perfect_match(self):
        """Identical results are paired correctly."""
        original = [CapturedData(data={"name": "Alice", "age": 30})]
        new = [CapturedData(data={"name": "Alice", "age": 30})]

        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            original, new
        )

        assert len(paired) == 1
        assert unpaired_orig == []
        assert unpaired_new == []

    def test_close_match(self):
        """Results with small differences are paired."""
        original = [CapturedData(data={"name": "Alice", "age": 30})]
        new = [CapturedData(data={"name": "Alice", "age": 31})]

        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            original, new
        )

        assert len(paired) == 1
        assert unpaired_orig == []
        assert unpaired_new == []

    def test_multiple_results_greedy_pairing(self):
        """Greedy pairing chooses closest match first."""
        original = [
            CapturedData(data={"id": 1, "value": "A"}),
            CapturedData(data={"id": 2, "value": "B"}),
        ]
        new = [
            CapturedData(data={"id": 1, "value": "A"}),  # Perfect match
            CapturedData(data={"id": 2, "value": "C"}),  # Close match
        ]

        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            original, new
        )

        assert len(paired) == 2
        assert unpaired_orig == []
        assert unpaired_new == []

    def test_unequal_counts_some_unpaired(self):
        """Extra results remain unpaired."""
        original = [
            CapturedData(data={"id": 1}),
        ]
        new = [
            CapturedData(data={"id": 1}),
            CapturedData(data={"id": 2}),
        ]

        paired, unpaired_orig, unpaired_new = _pair_results_by_levenshtein(
            original, new
        )

        assert len(paired) == 1
        assert unpaired_orig == []
        assert len(unpaired_new) == 1


class TestCompareDicts:
    """Test dict comparison with field-level diffs."""

    def test_identical_dicts(self):
        """Identical dicts have no diffs."""
        d1 = {"name": "Alice", "age": 30}
        d2 = {"name": "Alice", "age": 30}
        diffs = _compare_dicts(d1, d2)
        assert diffs == {}

    def test_changed_value(self):
        """Changed values are reported."""
        d1 = {"name": "Alice", "age": 30}
        d2 = {"name": "Alice", "age": 31}
        diffs = _compare_dicts(d1, d2)
        assert diffs == {"age": (30, 31)}

    def test_added_field(self):
        """Added fields are reported."""
        d1 = {"name": "Alice"}
        d2 = {"name": "Alice", "age": 30}
        diffs = _compare_dicts(d1, d2)
        assert diffs == {"age": (None, 30)}

    def test_removed_field(self):
        """Removed fields are reported."""
        d1 = {"name": "Alice", "age": 30}
        d2 = {"name": "Alice"}
        diffs = _compare_dicts(d1, d2)
        assert diffs == {"age": (30, None)}

    def test_multiple_changes(self):
        """Multiple changes are all reported."""
        d1 = {"name": "Alice", "age": 30, "city": "NYC"}
        d2 = {"name": "Alice", "age": 31, "country": "USA"}
        diffs = _compare_dicts(d1, d2)
        assert diffs == {
            "age": (30, 31),
            "city": ("NYC", None),
            "country": (None, "USA"),
        }


class TestCompareErrors:
    """Test error state comparison."""

    def test_no_errors(self):
        """No errors in either execution."""
        diff = _compare_errors(None, None)
        assert diff.status == "no_change"
        assert not diff.has_change

    def test_error_introduced(self):
        """Error present in new code but not original."""
        new_error = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        diff = _compare_errors(None, new_error)
        assert diff.status == "introduced"
        assert diff.has_change
        assert diff.new_error == new_error
        assert diff.original_error is None

    def test_error_resolved(self):
        """Error present in original but not new code."""
        orig_error = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        diff = _compare_errors(orig_error, None)
        assert diff.status == "resolved"
        assert diff.has_change
        assert diff.original_error == orig_error
        assert diff.new_error is None

    def test_same_error(self):
        """Same error in both executions."""
        error1 = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        error2 = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        diff = _compare_errors(error1, error2)
        assert diff.status == "no_change"
        assert not diff.has_change

    def test_error_changed_type(self):
        """Error type changed."""
        orig_error = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        new_error = CapturedError(
            error_type="TypeError", error_message="Invalid input"
        )
        diff = _compare_errors(orig_error, new_error)
        assert diff.status == "changed"
        assert diff.has_change

    def test_error_changed_message(self):
        """Error message changed."""
        orig_error = CapturedError(
            error_type="ValueError", error_message="Invalid input"
        )
        new_error = CapturedError(
            error_type="ValueError", error_message="Bad value"
        )
        diff = _compare_errors(orig_error, new_error)
        assert diff.status == "changed"
        assert diff.has_change


class TestCompareContinuationOutput:
    """Test full continuation output comparison."""

    def _make_request(self, url: str, continuation: str) -> CapturedRequest:
        """Helper to create a test CapturedRequest."""
        return CapturedRequest(
            request_type="navigating",
            url=url,
            method="GET",
            continuation=continuation,
            accumulated_data={},
            aux_data={},
            permanent={},
            current_location="",
            priority=9,
            deduplication_key=None,
            is_speculative=False,
            speculation_id=None,
        )

    def test_identical_outputs(self):
        """Completely identical outputs."""
        original = DryRunResult(
            requests=[self._make_request("http://example.com", "parse")],
            data=[CapturedData(data={"id": 1})],
            error=None,
        )
        new = DryRunResult(
            requests=[self._make_request("http://example.com", "parse")],
            data=[CapturedData(data={"id": 1})],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert result.is_identical
        assert not result.has_changes
        assert result.request_diff.unchanged_count == 1
        assert result.data_diff.identical_pairs == 1

    def test_request_added(self):
        """New code generates additional request."""
        original = DryRunResult(
            requests=[self._make_request("http://example.com/1", "parse")],
            data=[],
            error=None,
        )
        new = DryRunResult(
            requests=[
                self._make_request("http://example.com/1", "parse"),
                self._make_request("http://example.com/2", "parse"),
            ],
            data=[],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert len(result.request_diff.added) == 1
        assert result.request_diff.added[0].url == "http://example.com/2"

    def test_request_removed(self):
        """New code generates fewer requests."""
        original = DryRunResult(
            requests=[
                self._make_request("http://example.com/1", "parse"),
                self._make_request("http://example.com/2", "parse"),
            ],
            data=[],
            error=None,
        )
        new = DryRunResult(
            requests=[self._make_request("http://example.com/1", "parse")],
            data=[],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert len(result.request_diff.removed) == 1
        assert result.request_diff.removed[0].url == "http://example.com/2"

    def test_data_added(self):
        """New code yields more data."""
        original = DryRunResult(
            requests=[],
            data=[CapturedData(data={"id": 1})],
            error=None,
        )
        new = DryRunResult(
            requests=[],
            data=[
                CapturedData(data={"id": 1}),
                CapturedData(data={"id": 2}),
            ],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert len(result.data_diff.added) == 1
        assert result.data_diff.added[0].data == {"id": 2}

    def test_data_removed(self):
        """New code yields less data."""
        original = DryRunResult(
            requests=[],
            data=[
                CapturedData(data={"id": 1}),
                CapturedData(data={"id": 2}),
            ],
            error=None,
        )
        new = DryRunResult(
            requests=[],
            data=[CapturedData(data={"id": 1})],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert len(result.data_diff.removed) == 1
        assert result.data_diff.removed[0].data == {"id": 2}

    def test_data_changed(self):
        """Paired data has field changes."""
        original = DryRunResult(
            requests=[],
            data=[CapturedData(data={"id": 1, "name": "Alice"})],
            error=None,
        )
        new = DryRunResult(
            requests=[],
            data=[CapturedData(data={"id": 1, "name": "Bob"})],
            error=None,
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert len(result.data_diff.changed_pairs) == 1
        _, _, field_diffs = result.data_diff.changed_pairs[0]
        assert field_diffs == {"name": ("Alice", "Bob")}

    def test_error_introduced(self):
        """New code raises error."""
        original = DryRunResult(requests=[], data=[], error=None)
        new = DryRunResult(
            requests=[],
            data=[],
            error=CapturedError(
                error_type="ValueError", error_message="Bad input"
            ),
        )

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert result.error_diff.status == "introduced"

    def test_error_resolved(self):
        """New code fixes error."""
        original = DryRunResult(
            requests=[],
            data=[],
            error=CapturedError(
                error_type="ValueError", error_message="Bad input"
            ),
        )
        new = DryRunResult(requests=[], data=[], error=None)

        result = compare_continuation_output(
            request_id=1,
            request_url="http://test.com",
            continuation="initial",
            original=original,
            new=new,
        )

        assert not result.is_identical
        assert result.has_changes
        assert result.error_diff.status == "resolved"


class TestComparisonSummary:
    """Test summary statistics aggregation."""

    def test_empty_summary(self):
        """Empty summary has all zeros."""
        summary = ComparisonSummary()
        assert summary.total_requests == 0
        assert summary.identical_outputs == 0

    def test_add_identical_result(self):
        """Adding identical result updates counters."""
        summary = ComparisonSummary()

        result = ComparisonResult(
            request_id=1,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(unchanged_count=1),
            data_diff=DataDiff(identical_pairs=1),
            error_diff=ErrorDiff(status="no_change"),
        )

        summary.add_comparison(result)

        assert summary.total_requests == 1
        assert summary.identical_outputs == 1
        assert summary.requests_with_request_changes == 0
        assert summary.requests_with_data_changes == 0

    def test_add_result_with_request_changes(self):
        """Adding result with request changes updates counters."""
        summary = ComparisonSummary()

        req = CapturedRequest(
            request_type="navigating",
            url="http://example.com",
            method="GET",
            continuation="parse",
            accumulated_data={},
            aux_data={},
            permanent={},
            current_location="",
            priority=9,
            deduplication_key=None,
            is_speculative=False,
            speculation_id=None,
        )

        result = ComparisonResult(
            request_id=1,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(added=[req], removed=[req]),
            data_diff=DataDiff(),
            error_diff=ErrorDiff(status="no_change"),
        )

        summary.add_comparison(result)

        assert summary.total_requests == 1
        assert summary.identical_outputs == 0
        assert summary.requests_with_request_changes == 1
        assert summary.total_request_adds == 1
        assert summary.total_request_removes == 1

    def test_add_result_with_data_changes(self):
        """Adding result with data changes updates counters."""
        summary = ComparisonSummary()

        data = CapturedData(data={"id": 1})

        result = ComparisonResult(
            request_id=1,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(),
            data_diff=DataDiff(
                added=[data],
                removed=[data],
                changed_pairs=[(data, data, {"name": ("A", "B")})],
            ),
            error_diff=ErrorDiff(status="no_change"),
        )

        summary.add_comparison(result)

        assert summary.total_requests == 1
        assert summary.identical_outputs == 0
        assert summary.requests_with_data_changes == 1
        assert summary.total_data_adds == 1
        assert summary.total_data_removes == 1
        assert summary.total_data_changes == 1

    def test_add_result_with_error_introduced(self):
        """Adding result with introduced error updates counters."""
        summary = ComparisonSummary()

        result = ComparisonResult(
            request_id=1,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(),
            data_diff=DataDiff(),
            error_diff=ErrorDiff(status="introduced"),
        )

        summary.add_comparison(result)

        assert summary.total_requests == 1
        assert summary.errors_introduced == 1
        assert summary.errors_resolved == 0
        assert summary.errors_changed == 0

    def test_add_multiple_results(self):
        """Adding multiple results aggregates correctly."""
        summary = ComparisonSummary()

        # Add identical result
        result1 = ComparisonResult(
            request_id=1,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(),
            data_diff=DataDiff(identical_pairs=1),
            error_diff=ErrorDiff(status="no_change"),
        )

        # Add result with changes
        result2 = ComparisonResult(
            request_id=2,
            request_url="http://test.com",
            continuation="parse",
            request_diff=RequestDiff(
                added=[
                    CapturedRequest(
                        request_type="navigating",
                        url="http://example.com",
                        method="GET",
                        continuation="parse",
                        accumulated_data={},
                        aux_data={},
                        permanent={},
                        current_location="",
                        priority=9,
                        deduplication_key=None,
                        is_speculative=False,
                        speculation_id=None,
                    )
                ]
            ),
            data_diff=DataDiff(),
            error_diff=ErrorDiff(status="resolved"),
        )

        summary.add_comparison(result1)
        summary.add_comparison(result2)

        assert summary.total_requests == 2
        assert summary.identical_outputs == 1
        assert summary.requests_with_request_changes == 1
        assert summary.total_request_adds == 1
        assert summary.errors_resolved == 1
