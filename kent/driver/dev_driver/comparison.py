"""Comparison logic for continuation output analysis.

This module provides functionality for comparing scraper continuation outputs
between original stored results and dry-run replays with modified code. It
enables developers to understand how code changes affect scraper behavior.

The comparison system supports:
- Transitive child request tree comparison via parent_request_id
- Levenshtein-based pairing of ParsedData results
- Exact dict comparison with field-level diffs
- Detection of added/removed requests and data
- Error comparison (introduced/resolved/changed)
- Summary statistics generation

Key classes:
- ComparisonResult: Top-level comparison result for a single request
- RequestDiff: Differences in child request trees
- DataDiff: Differences in ParsedData outputs
- ErrorDiff: Differences in error states
- ComparisonSummary: Aggregate statistics across multiple comparisons
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

from kent.driver.dev_driver.dry_run_driver import (
    CapturedData,
    CapturedError,
    CapturedRequest,
    DryRunResult,
)


def _levenshtein_distance(s1: str, s2: str) -> int:
    """Calculate Levenshtein distance between two strings.

    Uses dynamic programming with O(min(m,n)) space complexity.

    Args:
        s1: First string.
        s2: Second string.

    Returns:
        Minimum number of single-character edits (insertions, deletions,
        or substitutions) needed to transform s1 into s2.
    """
    if len(s1) < len(s2):
        return _levenshtein_distance(s2, s1)

    if len(s2) == 0:
        return len(s1)

    previous_row: list[int] = list(range(len(s2) + 1))

    for i, c1 in enumerate(s1):
        current_row = [i + 1]
        for j, c2 in enumerate(s2):
            # Cost of insertions, deletions, or substitutions
            insertions = previous_row[j + 1] + 1
            deletions = current_row[j] + 1
            substitutions = previous_row[j] + (c1 != c2)
            current_row.append(min(insertions, deletions, substitutions))
        previous_row = current_row

    return previous_row[-1]


def _serialize_for_comparison(data: dict[str, Any]) -> str:
    """Serialize a data dict to a normalized JSON string for comparison.

    Args:
        data: Dictionary to serialize.

    Returns:
        JSON string with sorted keys for consistent comparison.
    """
    return json.dumps(data, sort_keys=True, default=str)


def _pair_results_by_levenshtein(
    original: list[CapturedData], new: list[CapturedData]
) -> tuple[
    list[tuple[CapturedData, CapturedData]],
    list[CapturedData],
    list[CapturedData],
]:
    """Pair original and new results using greedy Levenshtein distance matching.

    Uses greedy assignment: finds the pair with minimum distance, removes both
    from consideration, and repeats. This provides a simple and predictable
    pairing strategy.

    Optimization: Pre-computes all JSON serializations and identifies exact
    matches first to avoid expensive Levenshtein calculations when possible.

    Args:
        original: List of original CapturedData results.
        new: List of new CapturedData results.

    Returns:
        Tuple of (paired_results, unpaired_original, unpaired_new):
        - paired_results: List of (original, new) tuples for matched results
        - unpaired_original: Original results with no match (removed in new code)
        - unpaired_new: New results with no match (added in new code)
    """
    if not original and not new:
        return [], [], []

    if not original:
        return [], [], new.copy()

    if not new:
        return [], original.copy(), []

    # Pre-compute all JSON serializations once (major optimization)
    original_strs = [_serialize_for_comparison(o.data) for o in original]
    new_strs = [_serialize_for_comparison(n.data) for n in new]

    # Track which indices are still available
    available_orig: set[int] = set(range(len(original)))
    available_new: set[int] = set(range(len(new)))
    paired: list[tuple[CapturedData, CapturedData]] = []

    # Phase 1: Find exact matches first (O(n*m) string comparisons, no Levenshtein)
    # Build a map of new_str -> indices for fast lookup
    new_str_to_indices: dict[str, list[int]] = {}
    for j, ns in enumerate(new_strs):
        new_str_to_indices.setdefault(ns, []).append(j)

    for i in list(available_orig):
        orig_str = original_strs[i]
        if orig_str in new_str_to_indices:
            # Find first available new index with exact match
            for j in new_str_to_indices[orig_str]:
                if j in available_new:
                    paired.append((original[i], new[j]))
                    available_orig.discard(i)
                    available_new.discard(j)
                    break

    # Phase 2: Greedy Levenshtein pairing for remaining items
    # Pre-compute distance matrix for remaining items (computed once)
    if available_orig and available_new:
        orig_indices = sorted(available_orig)
        new_indices = sorted(available_new)

        # Compute distance matrix once
        distances: list[
            tuple[int, int, int]
        ] = []  # (distance, orig_idx, new_idx)
        for i in orig_indices:
            for j in new_indices:
                dist = _levenshtein_distance(original_strs[i], new_strs[j])
                distances.append((dist, i, j))

        # Sort by distance for greedy selection
        distances.sort(key=lambda x: x[0])

        # Greedily pair by smallest distance
        for _dist, i, j in distances:
            if i in available_orig and j in available_new:
                paired.append((original[i], new[j]))
                available_orig.discard(i)
                available_new.discard(j)

    # Collect unpaired items
    unpaired_original = [original[i] for i in sorted(available_orig)]
    unpaired_new = [new[j] for j in sorted(available_new)]

    return paired, unpaired_original, unpaired_new


def _compare_dicts(
    original: dict[str, Any], new: dict[str, Any]
) -> dict[str, tuple[Any, Any]]:
    """Compare two dicts and return field-level differences.

    Args:
        original: Original dict.
        new: New dict.

    Returns:
        Dictionary mapping field names to (old_value, new_value) tuples
        for fields that differ. Includes fields added/removed.
    """
    diffs: dict[str, tuple[Any, Any]] = {}

    # Check all keys in original
    for key in original:
        orig_value = original[key]
        if key not in new:
            # Field removed
            diffs[key] = (orig_value, None)
        elif original[key] != new[key]:
            # Field changed
            diffs[key] = (orig_value, new[key])

    # Check for added keys
    for key in new:
        if key not in original:
            diffs[key] = (None, new[key])

    return diffs


@dataclass
class RequestChange:
    """A single change in a request (added, removed, or modified).

    Attributes:
        change_type: Type of change ("added", "removed", "modified").
        request: The request data (original for removed, new for added/modified).
        original_request: For "modified", the original request data.
    """

    change_type: str  # "added", "removed", "modified"
    request: CapturedRequest
    original_request: CapturedRequest | None = None


@dataclass
class RequestDiff:
    """Differences in child request trees.

    Compares the transitive tree of requests generated by a continuation
    between original and new code.

    Attributes:
        added: Requests present in new code but not original.
        removed: Requests present in original but not in new code.
        modified: Requests with same identity but different parameters.
        unchanged_count: Number of requests that are identical.
    """

    added: list[CapturedRequest] = field(default_factory=list)
    removed: list[CapturedRequest] = field(default_factory=list)
    modified: list[tuple[CapturedRequest, CapturedRequest]] = field(
        default_factory=list
    )
    unchanged_count: int = 0

    @property
    def has_changes(self) -> bool:
        """Check if there are any differences."""
        return bool(self.added or self.removed or self.modified)

    @property
    def total_changes(self) -> int:
        """Total number of changes."""
        return len(self.added) + len(self.removed) + len(self.modified)


@dataclass
class DataDiff:
    """Differences in ParsedData outputs.

    Uses Levenshtein-based pairing to match results between original and
    new code, then performs exact dict comparison on paired results.

    Attributes:
        identical_pairs: Count of paired results with identical data.
        changed_pairs: List of (original, new, field_diffs) for changed data.
        added: Data present in new code but not original (unpaired new results).
        removed: Data present in original but not new code (unpaired original).
    """

    identical_pairs: int = 0
    changed_pairs: list[
        tuple[CapturedData, CapturedData, dict[str, tuple[Any, Any]]]
    ] = field(default_factory=list)
    added: list[CapturedData] = field(default_factory=list)
    removed: list[CapturedData] = field(default_factory=list)

    @property
    def has_changes(self) -> bool:
        """Check if there are any differences."""
        return bool(self.changed_pairs or self.added or self.removed)

    @property
    def total_pairs(self) -> int:
        """Total number of paired results (identical + changed)."""
        return self.identical_pairs + len(self.changed_pairs)


@dataclass
class ErrorDiff:
    """Differences in error states.

    Compares whether errors occurred and how they changed between
    original and new code.

    Attributes:
        status: Type of error change ("no_change", "introduced", "resolved", "changed").
        original_error: Error from original execution (if any).
        new_error: Error from new execution (if any).
    """

    status: str  # "no_change", "introduced", "resolved", "changed"
    original_error: CapturedError | None = None
    new_error: CapturedError | None = None

    @property
    def has_change(self) -> bool:
        """Check if there was an error state change."""
        return self.status != "no_change"


@dataclass
class ComparisonResult:
    """Complete comparison result for a single request.

    Compares the full transitive output of a continuation between
    original stored results and dry-run replay with new code.

    Attributes:
        request_id: ID of the request that was compared.
        request_url: URL of the request.
        continuation: Continuation method name.
        request_diff: Differences in child request trees.
        data_diff: Differences in ParsedData outputs.
        error_diff: Differences in error states.
    """

    request_id: int
    request_url: str
    continuation: str
    request_diff: RequestDiff
    data_diff: DataDiff
    error_diff: ErrorDiff

    @property
    def has_changes(self) -> bool:
        """Check if there are any differences."""
        return (
            self.request_diff.has_changes
            or self.data_diff.has_changes
            or self.error_diff.has_change
        )

    @property
    def is_identical(self) -> bool:
        """Check if outputs are completely identical."""
        return not self.has_changes


@dataclass
class ComparisonSummary:
    """Aggregate statistics across multiple comparisons.

    Provides high-level metrics for understanding the impact of code
    changes across many requests.

    Attributes:
        total_requests: Total number of requests compared.
        identical_outputs: Requests with no differences at all.
        requests_with_request_changes: Requests with child request tree changes.
        requests_with_data_changes: Requests with ParsedData changes.
        errors_introduced: Count of new errors in new code.
        errors_resolved: Count of original errors fixed in new code.
        errors_changed: Count of errors that changed type/message.
        total_request_adds: Total added child requests across all comparisons.
        total_request_removes: Total removed child requests.
        total_request_modifications: Total modified child requests.
        total_data_adds: Total added ParsedData results.
        total_data_removes: Total removed ParsedData results.
        total_data_changes: Total changed ParsedData results (within pairs).
    """

    total_requests: int = 0
    identical_outputs: int = 0
    requests_with_request_changes: int = 0
    requests_with_data_changes: int = 0
    errors_introduced: int = 0
    errors_resolved: int = 0
    errors_changed: int = 0
    total_request_adds: int = 0
    total_request_removes: int = 0
    total_request_modifications: int = 0
    total_data_adds: int = 0
    total_data_removes: int = 0
    total_data_changes: int = 0

    def add_comparison(self, result: ComparisonResult) -> None:
        """Add a comparison result to the summary statistics.

        Args:
            result: The comparison result to incorporate.
        """
        self.total_requests += 1

        if result.is_identical:
            self.identical_outputs += 1

        if result.request_diff.has_changes:
            self.requests_with_request_changes += 1
            self.total_request_adds += len(result.request_diff.added)
            self.total_request_removes += len(result.request_diff.removed)
            self.total_request_modifications += len(
                result.request_diff.modified
            )

        if result.data_diff.has_changes:
            self.requests_with_data_changes += 1
            self.total_data_adds += len(result.data_diff.added)
            self.total_data_removes += len(result.data_diff.removed)
            self.total_data_changes += len(result.data_diff.changed_pairs)

        if result.error_diff.status == "introduced":
            self.errors_introduced += 1
        elif result.error_diff.status == "resolved":
            self.errors_resolved += 1
        elif result.error_diff.status == "changed":
            self.errors_changed += 1


def compare_continuation_output(
    request_id: int,
    request_url: str,
    continuation: str,
    original: DryRunResult,
    new: DryRunResult,
) -> ComparisonResult:
    """Compare continuation outputs between original and new code.

    Performs a comprehensive comparison of all outputs from a continuation:
    - Child request tree (transitive via parent_request_id)
    - ParsedData yields (paired by Levenshtein distance)
    - Error states

    Args:
        request_id: ID of the request being compared.
        request_url: URL of the request.
        continuation: Continuation method name.
        original: DryRunResult from original code (loaded from database).
        new: DryRunResult from new code (dry-run replay).

    Returns:
        ComparisonResult with detailed diffs across all dimensions.
    """
    # Compare requests
    request_diff = _compare_requests(original.requests, new.requests)

    # Compare data using Levenshtein pairing
    data_diff = _compare_data(original.data, new.data)

    # Compare errors
    error_diff = _compare_errors(original.error, new.error)

    return ComparisonResult(
        request_id=request_id,
        request_url=request_url,
        continuation=continuation,
        request_diff=request_diff,
        data_diff=data_diff,
        error_diff=error_diff,
    )


def _normalize_url(url: str) -> str:
    """Normalize URL for comparison by decoding percent-encoded characters.

    This ensures URLs like "...sort=field%2Casc" and "...sort=field,asc"
    are treated as equivalent.
    """
    return unquote(url)


def _compare_requests(
    original: list[CapturedRequest], new: list[CapturedRequest]
) -> RequestDiff:
    """Compare two lists of captured requests.

    Identifies added, removed, and modified requests. Requests are matched
    by deduplication_key (which is the canonical identifier for request equivalence).
    Falls back to normalized URL + continuation if deduplication_key is None.

    Args:
        original: Requests from original code.
        new: Requests from new code.

    Returns:
        RequestDiff summarizing the differences.
    """
    diff = RequestDiff()

    def get_key(req: CapturedRequest) -> str | tuple[str, str]:
        """Get the matching key for a request."""
        if req.deduplication_key is not None:
            return req.deduplication_key
        # Fallback to normalized URL + continuation
        return (_normalize_url(req.url), req.continuation)

    # Create lookup maps by deduplication_key (or fallback)
    original_map: dict[str | tuple[str, str], CapturedRequest] = {
        get_key(req): req for req in original
    }
    new_map: dict[str | tuple[str, str], CapturedRequest] = {
        get_key(req): req for req in new
    }

    # Find modified and unchanged requests
    for key, orig_req in original_map.items():
        if key in new_map:
            new_req = new_map[key]
            # Check if they're identical
            if _requests_equal(orig_req, new_req):
                diff.unchanged_count += 1
            else:
                diff.modified.append((orig_req, new_req))
        else:
            # Request was removed
            diff.removed.append(orig_req)

    # Find added requests
    for key, new_req in new_map.items():
        if key not in original_map:
            diff.added.append(new_req)

    return diff


def _requests_equal(req1: CapturedRequest, req2: CapturedRequest) -> bool:
    """Check if two captured requests are identical.

    Args:
        req1: First request.
        req2: Second request.

    Returns:
        True if all fields match, False otherwise.
    """
    return (
        req1.request_type == req2.request_type
        and _normalize_url(req1.url) == _normalize_url(req2.url)
        and req1.method == req2.method
        and req1.continuation == req2.continuation
        and req1.accumulated_data == req2.accumulated_data
        and req1.aux_data == req2.aux_data
        and req1.permanent == req2.permanent
        and req1.current_location == req2.current_location
        and req1.priority == req2.priority
        and req1.deduplication_key == req2.deduplication_key
        and req1.is_speculative == req2.is_speculative
        and req1.speculation_id == req2.speculation_id
        and req1.expected_type == req2.expected_type
    )


def _compare_data(
    original: list[CapturedData], new: list[CapturedData]
) -> DataDiff:
    """Compare ParsedData outputs using Levenshtein-based pairing.

    Args:
        original: Data from original code.
        new: Data from new code.

    Returns:
        DataDiff with paired comparisons and unpaired results.
    """
    diff = DataDiff()

    # Pair results by Levenshtein distance
    paired, unpaired_original, unpaired_new = _pair_results_by_levenshtein(
        original, new
    )

    # Compare paired results
    for orig_data, new_data in paired:
        field_diffs = _compare_dicts(orig_data.data, new_data.data)
        if field_diffs:
            diff.changed_pairs.append((orig_data, new_data, field_diffs))
        else:
            diff.identical_pairs += 1

    # Record unpaired results
    diff.removed = unpaired_original
    diff.added = unpaired_new

    return diff


def _compare_errors(
    original: CapturedError | None, new: CapturedError | None
) -> ErrorDiff:
    """Compare error states between original and new execution.

    Args:
        original: Error from original code (if any).
        new: Error from new code (if any).

    Returns:
        ErrorDiff describing the error state change.
    """
    if original is None and new is None:
        return ErrorDiff(status="no_change")

    if original is None and new is not None:
        return ErrorDiff(
            status="introduced", original_error=None, new_error=new
        )

    if original is not None and new is None:
        return ErrorDiff(
            status="resolved", original_error=original, new_error=None
        )

    # Both have errors - check if they changed
    assert original is not None and new is not None
    if (
        original.error_type == new.error_type
        and original.error_message == new.error_message
    ):
        return ErrorDiff(
            status="no_change", original_error=original, new_error=new
        )

    return ErrorDiff(status="changed", original_error=original, new_error=new)
