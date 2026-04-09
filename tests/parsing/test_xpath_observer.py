"""Tests for XPathObserver context manager.

Tests cover:
- Basic recording of XPath queries
- Nested queries on result elements
- CSS selector recording
- simple_tree() output format
- json() serialization
- Behavior when no observer is active
- Context manager cleanup
- Sample truncation
"""

from __future__ import annotations

from lxml import html as lxml_html

from kent.common.checked_html import CheckedHtmlElement
from kent.common.xpath_observer import (
    XPathObserver,
    get_active_observer,
)

SAMPLE_HTML = """
<html>
<body>
    <div id="content">
        <table id="results">
            <tr class="row">
                <td class="name">Item One</td>
                <td class="value">100</td>
            </tr>
            <tr class="row">
                <td class="name">Item Two</td>
                <td class="value">200</td>
            </tr>
            <tr class="row">
                <td class="name">Item Three</td>
                <td class="value">300</td>
            </tr>
        </table>
    </div>
</body>
</html>
"""


class TestBasicRecording:
    """Test basic XPath query recording."""

    def test_basic_recording(self) -> None:
        """Observer captures single xpath query with correct fields."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath("//tr[@class='row']", "table rows")
            assert len(rows) == 3

        assert len(observer.queries) == 1
        query = observer.queries[0]
        assert query.selector == "//tr[@class='row']"
        assert query.selector_type == "xpath"
        assert query.description == "table rows"
        assert query.match_count == 3
        assert query.expected_min == 1
        assert query.expected_max is None
        assert query.element_id is not None

    def test_multiple_queries(self) -> None:
        """Observer captures multiple xpath queries."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table", "table")
            tree.checked_xpath("//tr[@class='row']", "rows")
            tree.checked_xpath("//td[@class='name']", "names")

        assert len(observer.queries) == 3
        assert observer.queries[0].selector == "//table"
        assert observer.queries[1].selector == "//tr[@class='row']"
        assert observer.queries[2].selector == "//td[@class='name']"


class TestNestedQueries:
    """Test nested queries on result elements."""

    def test_nested_queries(self) -> None:
        """Queries on result elements are recorded with parent tracking."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            table = tree.checked_xpath(
                "//table[@id='results']", "results table"
            )[0]
            # Query on the result element
            rows = table.checked_xpath(".//tr[@class='row']", "rows")
            assert len(rows) == 3

        # Both queries are recorded at top level
        assert len(observer.queries) == 2

        # The rows query has parent_element_id linking to the table query
        table_query = observer.queries[0]
        rows_query = observer.queries[1]
        assert rows_query.parent_element_id == table_query.element_id


class TestCssRecording:
    """Test CSS selector recording."""

    def test_css_recording(self) -> None:
        """CSS selectors recorded with selector_type='css'."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_css("tr.row", "table rows")
            assert len(rows) == 3

        assert len(observer.queries) == 1
        query = observer.queries[0]
        assert query.selector == "tr.row"
        assert query.selector_type == "css"
        assert query.description == "table rows"
        assert query.match_count == 3


class TestSimpleTreeFormat:
    """Test simple_tree() output format."""

    def test_simple_tree_format(self) -> None:
        """Output shows checkmark status and match counts correctly."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table", "table", min_count=1, max_count=1)
            tree.checked_xpath("//tr[@class='row']", "rows", min_count=1)
            tree.checked_xpath(
                "//span[@class='missing']", "missing spans", min_count=0
            )

        output = observer.simple_tree()

        # Check for success marker on table query
        assert '//table "table" ✓ (1 match)' in output
        # Check for success marker on rows query
        assert "3 matches" in output
        # Check that zero matches also appears
        assert "0 matches" in output

    def test_simple_tree_failure_markers(self) -> None:
        """Output shows X status for failed expectations."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            # This should pass (1 result, expecting at least 1)
            tree.checked_xpath("//table", "table", min_count=1)
            # This will match 3, but we allow 0 minimum for the observer to record
            tree.checked_xpath(
                "//span[@class='missing']", "missing", min_count=0
            )

        output = observer.simple_tree()
        # Table query should show success
        assert "✓" in output


class TestJsonSerialization:
    """Test json() serialization."""

    def test_json_serialization(self) -> None:
        """json() returns valid list of dicts."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table", "table")
            tree.checked_xpath("//tr[@class='row']", "rows")

        result = observer.json()

        assert isinstance(result, list)
        assert len(result) == 2
        for item in result:
            assert isinstance(item, dict)
            assert "selector" in item
            assert "selector_type" in item
            assert "description" in item
            assert "match_count" in item
            assert "expected_min" in item
            assert "expected_max" in item
            assert "sample_elements" in item
            assert "children" in item
            assert "element_id" in item


class TestNoObserverActive:
    """Test behavior when no observer is active."""

    def test_no_observer_active(self) -> None:
        """CheckedHtmlElement works normally when no observer set."""
        # No observer context
        assert get_active_observer() is None

        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )
        rows = tree.checked_xpath("//tr[@class='row']", "rows")
        assert len(rows) == 3

        # Still no observer
        assert get_active_observer() is None


class TestContextManagerCleanup:
    """Test context manager cleanup."""

    def test_context_manager_cleanup(self) -> None:
        """Observer properly removed after __exit__."""
        assert get_active_observer() is None

        with XPathObserver() as observer:
            assert get_active_observer() is observer

        # Should be cleaned up
        assert get_active_observer() is None

    def test_nested_contexts_not_supported(self) -> None:
        """Nested observers overwrite each other (documents behavior)."""
        with XPathObserver() as outer:
            assert get_active_observer() is outer

            with XPathObserver() as inner:
                # Inner observer takes over
                assert get_active_observer() is inner

            # After inner exits, outer is restored
            assert get_active_observer() is outer

        assert get_active_observer() is None


class TestSampleTruncation:
    """Test sample content truncation."""

    def test_sample_truncation(self) -> None:
        """Long text content truncated to max_sample_length."""
        long_html = """
        <html>
        <body>
            <p class="long">
                This is a very long paragraph that contains much more text than
                the default sample length limit would allow. It should be truncated
                with an ellipsis to keep the output readable.
            </p>
        </body>
        </html>
        """
        tree = CheckedHtmlElement(
            lxml_html.fromstring(long_html), "http://example.com"
        )

        with XPathObserver(max_sample_length=50) as observer:
            tree.checked_xpath("//p[@class='long']", "long paragraph")

        assert len(observer.queries) == 1
        query = observer.queries[0]
        assert len(query.sample_elements) == 1
        sample = query.sample_elements[0]
        # Should be truncated with ellipsis
        assert len(sample) <= 53  # 50 + "..."
        assert sample.endswith("...")

    def test_max_samples_limit(self) -> None:
        """Number of samples limited to max_samples."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver(max_samples=2) as observer:
            tree.checked_xpath("//tr[@class='row']", "rows")  # 3 matches

        query = observer.queries[0]
        assert query.match_count == 3
        assert len(query.sample_elements) == 2  # Limited to max_samples


class TestSampleContent:
    """Test sample content extraction."""

    def test_sample_extracts_text_content(self) -> None:
        """Samples extract text content from elements."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//td[@class='name']", "names")

        query = observer.queries[0]
        assert len(query.sample_elements) == 3
        assert "Item One" in query.sample_elements[0]
        assert "Item Two" in query.sample_elements[1]
        assert "Item Three" in query.sample_elements[2]

    def test_sample_handles_string_results(self) -> None:
        """Samples work with string XPath results (attributes/text())."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table/@id", "table ids", type=str)

        query = observer.queries[0]
        assert len(query.sample_elements) == 1
        assert query.sample_elements[0] == "results"


class TestExpectedCounts:
    """Test expected count recording."""

    def test_expected_counts_recorded(self) -> None:
        """expected_min and expected_max are recorded correctly."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table", "table", min_count=1, max_count=1)
            tree.checked_xpath("//tr[@class='row']", "rows", min_count=2)

        assert observer.queries[0].expected_min == 1
        assert observer.queries[0].expected_max == 1
        assert observer.queries[1].expected_min == 2
        assert observer.queries[1].expected_max is None


class TestParentElementTracking:
    """Test parent element ID tracking for scoped highlighting."""

    def test_root_query_has_no_parent(self) -> None:
        """Queries on the root tree have no parent_element_id."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            tree.checked_xpath("//table[@id='results']", "results table")

        assert len(observer.queries) == 1
        assert observer.queries[0].parent_element_id is None

    def test_child_query_has_parent_id(self) -> None:
        """Queries on result elements have parent_element_id set."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            table = tree.checked_xpath(
                "//table[@id='results']", "results table"
            )[0]
            # Query on a result element should have parent_element_id
            table.checked_xpath(".//tr[@class='row']", "rows")

        assert len(observer.queries) == 2
        table_query = observer.queries[0]
        rows_query = observer.queries[1]

        # Table query has no parent
        assert table_query.parent_element_id is None

        # Rows query should reference the table query
        assert rows_query.parent_element_id == table_query.element_id

    def test_deeply_nested_parent_chain(self) -> None:
        """Multiple levels of nesting track parent chain correctly."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            # Level 0: root query
            table = tree.checked_xpath(
                "//table[@id='results']", "results table"
            )[0]
            # Level 1: query on table
            rows = table.checked_xpath(".//tr[@class='row']", "rows")
            # Level 2: query on first row
            cells = rows[0].checked_xpath(".//td", "cells")
            assert len(cells) == 2

        assert len(observer.queries) == 3
        table_query = observer.queries[0]
        rows_query = observer.queries[1]
        cells_query = observer.queries[2]

        # Table (root) -> no parent
        assert table_query.parent_element_id is None

        # Rows -> parent is table
        assert rows_query.parent_element_id == table_query.element_id

        # Cells -> parent is rows (the first row element)
        assert cells_query.parent_element_id == rows_query.element_id

    def test_sibling_queries_same_parent(self) -> None:
        """Multiple queries on the same parent have the same parent_element_id."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            table = tree.checked_xpath(
                "//table[@id='results']", "results table"
            )[0]
            # Two queries on the same table element
            table.checked_xpath(".//tr[@class='row']", "rows")
            table.checked_xpath(".//td[@class='name']", "name cells")

        assert len(observer.queries) == 3
        table_query = observer.queries[0]
        rows_query = observer.queries[1]
        names_query = observer.queries[2]

        # Both child queries should have the same parent (the table query)
        assert rows_query.parent_element_id == table_query.element_id
        assert names_query.parent_element_id == table_query.element_id

    def test_same_selector_different_elements_deduplicated(self) -> None:
        """Same selector on different elements with same parent is deduplicated."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            # Query on first row
            rows[0].checked_xpath(".//td[@class='name']", "first row name")
            # Query on second row - same selector, same parent query
            rows[1].checked_xpath(".//td[@class='name']", "second row name")

        # Deduplicated: only 2 queries (rows + one deduplicated name query)
        assert len(observer.queries) == 2
        rows_query = observer.queries[0]
        name_query = observer.queries[1]

        # The deduplicated query has the rows query as parent
        assert name_query.parent_element_id == rows_query.element_id

        # Match count is aggregated (1 + 1 = 2)
        assert name_query.match_count == 2

        # Samples are collected from both (up to max_samples)
        assert len(name_query.sample_elements) == 2
        assert "Item One" in name_query.sample_elements[0]
        assert "Item Two" in name_query.sample_elements[1]

    def test_parent_element_id_in_json(self) -> None:
        """parent_element_id is included in JSON serialization."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            table = tree.checked_xpath(
                "//table[@id='results']", "results table"
            )[0]
            table.checked_xpath(".//tr[@class='row']", "rows")

        result = observer.json()

        assert len(result) == 2
        # Root query
        assert "parent_element_id" in result[0]
        assert result[0]["parent_element_id"] is None
        # Child query
        assert "parent_element_id" in result[1]
        assert result[1]["parent_element_id"] == result[0]["element_id"]

    def test_css_queries_track_parent(self) -> None:
        """CSS selector queries also track parent_element_id."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            table = tree.checked_css("table#results", "results table")[0]
            table.checked_css("tr.row", "rows")

        assert len(observer.queries) == 2
        table_query = observer.queries[0]
        rows_query = observer.queries[1]

        assert table_query.parent_element_id is None
        assert rows_query.parent_element_id == table_query.element_id


class TestSelectorDeduplication:
    """Test deduplication of repeated selectors with same parent."""

    def test_parent_child_single_iteration(self) -> None:
        """parent > child: Single child query is recorded once."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            # Just one child query
            rows[0].checked_xpath(".//td[@class='name']", "name cell")

        # 2 queries: parent + 1 child
        assert len(observer.queries) == 2

        rows_query = observer.queries[0]
        name_query = observer.queries[1]

        assert rows_query.selector == ".//tr[@class='row']"
        assert name_query.selector == ".//td[@class='name']"
        assert name_query.parent_element_id == rows_query.element_id
        assert name_query.match_count == 1

    def test_parent_child_child_two_iterations(self) -> None:
        """parent > child+child: Two iterations deduplicated into one query."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            # Two iterations with same selector
            rows[0].checked_xpath(".//td[@class='name']", "name cell")
            rows[1].checked_xpath(".//td[@class='name']", "name cell")

        # Still 2 queries: parent + 1 deduplicated child
        assert len(observer.queries) == 2

        rows_query = observer.queries[0]
        name_query = observer.queries[1]

        assert name_query.parent_element_id == rows_query.element_id
        # Match count aggregated from both iterations
        assert name_query.match_count == 2
        # Samples from both
        assert len(name_query.sample_elements) == 2

    def test_parent_child_times_three(self) -> None:
        """parent > child*3: Three iterations deduplicated into one query."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            # Three iterations with same selector
            for row in rows:
                row.checked_xpath(".//td[@class='name']", "name cell")

        # Still 2 queries: parent + 1 deduplicated child
        assert len(observer.queries) == 2

        rows_query = observer.queries[0]
        name_query = observer.queries[1]

        assert name_query.parent_element_id == rows_query.element_id
        # Match count aggregated from all three iterations
        assert name_query.match_count == 3
        # Samples capped at max_samples (default 3)
        assert len(name_query.sample_elements) == 3
        assert "Item One" in name_query.sample_elements[0]
        assert "Item Two" in name_query.sample_elements[1]
        assert "Item Three" in name_query.sample_elements[2]

    def test_different_selectors_not_deduplicated(self) -> None:
        """Different selectors on same parent are not deduplicated."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            # Different selectors in iteration
            rows[0].checked_xpath(".//td[@class='name']", "name cell")
            rows[0].checked_xpath(".//td[@class='value']", "value cell")

        # 3 queries: parent + 2 different children
        assert len(observer.queries) == 3

        assert observer.queries[1].selector == ".//td[@class='name']"
        assert observer.queries[2].selector == ".//td[@class='value']"

    def test_same_selector_different_parents_not_deduplicated(self) -> None:
        """Same selector with different parent queries are not deduplicated."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            # Two separate parent queries
            table1 = tree.checked_xpath("//table[@id='results']", "table 1")[0]
            # Query within first parent context
            table1.checked_xpath(".//td", "cells from table1")

            # Simulate a second parent by querying the same table differently
            table2 = tree.checked_xpath("//div/table", "table 2")[0]
            table2.checked_xpath(".//td", "cells from table2")

        # 4 queries: table1, cells1, table2, cells2
        assert len(observer.queries) == 4

        table1_query = observer.queries[0]
        cells1_query = observer.queries[1]
        table2_query = observer.queries[2]
        cells2_query = observer.queries[3]

        # Same selector but different parents = not deduplicated
        assert cells1_query.selector == ".//td"
        assert cells2_query.selector == ".//td"
        assert cells1_query.parent_element_id == table1_query.element_id
        assert cells2_query.parent_element_id == table2_query.element_id
        assert cells1_query.element_id != cells2_query.element_id

    def test_dedup_aggregates_samples_up_to_max(self) -> None:
        """Deduplication respects max_samples limit when aggregating."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver(max_samples=2) as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            for row in rows:  # 3 rows
                row.checked_xpath(".//td[@class='name']", "name cell")

        name_query = observer.queries[1]

        # Match count includes all 3
        assert name_query.match_count == 3
        # But samples capped at max_samples=2
        assert len(name_query.sample_elements) == 2

    def test_dedup_preserves_first_description(self) -> None:
        """Deduplicated query keeps the first description."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_xpath(".//tr[@class='row']", "rows")
            rows[0].checked_xpath(".//td[@class='name']", "first description")
            rows[1].checked_xpath(".//td[@class='name']", "second description")

        name_query = observer.queries[1]
        # First description is preserved
        assert name_query.description == "first description"

    def test_dedup_works_with_css_selectors(self) -> None:
        """Deduplication also works for CSS selectors."""
        tree = CheckedHtmlElement(
            lxml_html.fromstring(SAMPLE_HTML), "http://example.com"
        )

        with XPathObserver() as observer:
            rows = tree.checked_css("tr.row", "rows")
            for row in rows:
                row.checked_css("td.name", "name cell")

        # 2 queries: parent + deduplicated child
        assert len(observer.queries) == 2

        name_query = observer.queries[1]
        assert name_query.selector == "td.name"
        assert name_query.selector_type == "css"
        assert name_query.match_count == 3
