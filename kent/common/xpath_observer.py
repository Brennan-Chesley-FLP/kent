"""XPath/CSS observer for debugging selector queries.

This module provides XPathObserver, a context manager that instruments
CheckedHtmlElement to collect XPath/CSS query data. This is useful for
debugging scrapers when HTML structure changes or XPath queries are incorrect.

Usage::

    from kent.common.xpath_observer import XPathObserver

    with XPathObserver() as observer:
        tree = CheckedHtmlElement(lxml_html.fromstring(content), url)
        rows = tree.checked_xpath("//tr", "table rows", min_count=1)
        for row in rows:
            cells = row.checked_xpath(".//td", "cells")

        print(observer.simple_tree())  # Human-readable tree
        print(observer.json())  # JSON for UI highlighting
"""

from __future__ import annotations

import contextvars
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from lxml.html import HtmlElement

# Context variable for the active observer
_active_observer: contextvars.ContextVar[XPathObserver | None] = (
    contextvars.ContextVar("xpath_observer", default=None)
)


@dataclass
class SelectorQuery:
    """A single XPath or CSS selector query."""

    selector: str
    selector_type: str  # "xpath" or "css"
    description: str
    match_count: int
    expected_min: int
    expected_max: int | None
    sample_elements: list[str] = field(default_factory=list)
    children: list[SelectorQuery] = field(default_factory=list)
    element_id: str | None = None  # Unique ID for highlighting in UI
    parent_element_id: str | None = (
        None  # ID of parent query (for scoped highlights)
    )

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "selector": self.selector,
            "selector_type": self.selector_type,
            "description": self.description,
            "match_count": self.match_count,
            "expected_min": self.expected_min,
            "expected_max": self.expected_max,
            "sample_elements": self.sample_elements,
            "children": [c.to_dict() for c in self.children],
            "element_id": self.element_id,
            "parent_element_id": self.parent_element_id,
        }


class XPathObserver:
    """Observer that collects XPath/CSS query information.

    Usage::

        with XPathObserver() as observer:
            tree = CheckedHtmlElement(lxml_html.fromstring(content), url)
            rows = tree.checked_xpath("//tr", "table rows", min_count=1)
            for row in rows:
                cells = row.checked_xpath(".//td", "cells")

            print(observer.simple_tree())  # Human-readable tree
            print(observer.json())  # JSON for UI highlighting

    Deduplication:

    When the same selector is used multiple times with the same parent query
    (e.g., iterating over rows and selecting the same column from each),
    the observer deduplicates these into a single query entry. Match counts
    and sample elements are aggregated.
    """

    def __init__(self, max_sample_length: int = 100, max_samples: int = 3):
        """Initialize the observer.

        Args:
            max_sample_length: Maximum characters per sample element.
            max_samples: Maximum number of sample elements to capture.
        """
        self.max_sample_length = max_sample_length
        self.max_samples = max_samples
        self.queries: list[SelectorQuery] = []
        self._query_stack: list[SelectorQuery] = []
        self._element_counter: int = 0
        self._token: contextvars.Token[XPathObserver | None] | None = None
        # Maps element id() to the query that produced it
        self._element_to_query: dict[int, SelectorQuery] = {}
        # Maps (parent_element_id, selector) to existing SelectorQuery for deduplication
        self._dedup_index: dict[tuple[str | None, str], SelectorQuery] = {}

    def __enter__(self) -> XPathObserver:
        """Enter the observer context."""
        self._token = _active_observer.set(self)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: Any,
    ) -> None:
        """Exit the observer context."""
        if self._token is not None:
            _active_observer.reset(self._token)
            self._token = None

    def record_query(
        self,
        selector: str,
        selector_type: str,
        description: str,
        results: list[Any],
        expected_min: int,
        expected_max: int | None,
        parent_element: HtmlElement | None = None,
    ) -> None:
        """Record a selector query and its results.

        Args:
            selector: The XPath or CSS selector string.
            selector_type: "xpath" or "css".
            description: Human-readable description from checked_xpath/css.
            results: The elements/values returned by the query.
            expected_min: Minimum expected count.
            expected_max: Maximum expected count (None = unlimited).
            parent_element: The element the query was executed on.

        Note:
            Queries with the same (parent_element_id, selector) are deduplicated.
            Match counts and samples are aggregated into the existing query.
        """
        # Find parent query if this query was executed on a child element
        parent_query_id: str | None = None
        if parent_element is not None:
            parent_elem_id = id(parent_element)
            parent_query = self._element_to_query.get(parent_elem_id)
            if parent_query is not None:
                parent_query_id = parent_query.element_id

        # Check for existing query with same parent + selector (deduplication)
        dedup_key = (parent_query_id, selector)
        existing_query = self._dedup_index.get(dedup_key)

        if existing_query is not None:
            # Aggregate into existing query
            existing_query.match_count += len(results)

            # Add more samples if we haven't hit the limit
            samples_needed = self.max_samples - len(
                existing_query.sample_elements
            )
            if samples_needed > 0:
                new_samples = self._extract_samples(results[:samples_needed])
                existing_query.sample_elements.extend(new_samples)

            # Track which elements came from this query (use existing query)
            for result in results:
                elem = self._unwrap_element(result)
                if elem is not None:
                    self._element_to_query[id(elem)] = existing_query

            return

        # Generate sample content from results
        samples = self._extract_samples(results[: self.max_samples])

        # Generate unique element ID for highlighting
        self._element_counter += 1
        element_id = f"xpath_match_{self._element_counter}"

        query = SelectorQuery(
            selector=selector,
            selector_type=selector_type,
            description=description,
            match_count=len(results),
            expected_min=expected_min,
            expected_max=expected_max,
            sample_elements=samples,
            element_id=element_id,
            parent_element_id=parent_query_id,
        )

        # Register in dedup index
        self._dedup_index[dedup_key] = query

        # Track which elements came from this query (for future child queries)
        for result in results:
            elem = self._unwrap_element(result)
            if elem is not None:
                self._element_to_query[id(elem)] = query

        # Add to current context (nested or top-level)
        if self._query_stack:
            self._query_stack[-1].children.append(query)
        else:
            self.queries.append(query)

    def _unwrap_element(self, result: Any) -> Any | None:
        """Unwrap a result to get the underlying HtmlElement."""
        if hasattr(result, "_element"):
            return result._element
        elif hasattr(result, "tag"):  # HtmlElement
            return result
        return None

    def _extract_samples(self, results: list[Any]) -> list[str]:
        """Extract sample text content from results."""
        samples = []
        for result in results:
            if hasattr(result, "text_content"):
                # HtmlElement - get text content
                text = result.text_content()
            elif hasattr(result, "_element") and hasattr(
                result._element, "text_content"
            ):
                # CheckedHtmlElement wrapper
                text = result._element.text_content()
            elif isinstance(result, str):
                text = result
            else:
                text = str(result)

            # Normalize whitespace and truncate
            text = " ".join(text.split())
            if len(text) > self.max_sample_length:
                text = text[: self.max_sample_length] + "..."
            samples.append(text)
        return samples

    def push_context(self, query: SelectorQuery) -> None:
        """Push a query onto the stack for nested queries."""
        self._query_stack.append(query)

    def pop_context(self) -> SelectorQuery | None:
        """Pop a query from the stack."""
        if self._query_stack:
            return self._query_stack.pop()
        return None

    def simple_tree(self, indent: int = 0) -> str:
        """Generate a human-readable tree representation.

        Returns:
            Formatted string showing query hierarchy with match counts.

        Example output::

            - //div[@id='mainContent']/table "Main Table" ✓ (1 match)
              - //tr "Main Table Rows" ✓ (5 matches)
                - (//td)[2] "Important Column" ✗ (0 matches, expected 1+)
        """
        lines = []
        for query in self.queries:
            lines.extend(self._format_query(query, indent))
        return "\n".join(lines)

    def _format_query(self, query: SelectorQuery, indent: int) -> list[str]:
        """Format a single query and its children."""
        prefix = "  " * indent + "- "

        # Status indicator
        if query.match_count >= query.expected_min:
            if (
                query.expected_max is None
                or query.match_count <= query.expected_max
            ):
                status = "✓"
            else:
                status = "✗"
        else:
            status = "✗"

        # Match count display
        match_text = f"{query.match_count} match" + (
            "es" if query.match_count != 1 else ""
        )
        if status == "✗":
            if query.match_count < query.expected_min:
                match_text += f", expected {query.expected_min}+"
            elif query.expected_max and query.match_count > query.expected_max:
                match_text += f", expected max {query.expected_max}"

        line = f'{prefix}{query.selector} "{query.description}" {status} ({match_text})'
        lines = [line]

        # Add sample content preview if available
        if query.sample_elements and query.match_count > 0:
            sample_preview = query.sample_elements[0]
            if sample_preview:
                sample_line = "  " * (indent + 1) + f'→ "{sample_preview}"'
                lines.append(sample_line)

        # Recurse for children
        for child in query.children:
            lines.extend(self._format_query(child, indent + 1))

        return lines

    def json(self) -> list[dict[str, Any]]:
        """Generate JSON representation for UI highlighting.

        Returns:
            List of query dictionaries suitable for JavaScript processing.
        """
        return [q.to_dict() for q in self.queries]


def get_active_observer() -> XPathObserver | None:
    """Get the currently active XPathObserver, if any."""
    return _active_observer.get()
