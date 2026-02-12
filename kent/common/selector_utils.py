"""Selector utility functions for driver integration.

This module provides utilities that enable drivers to work with selectors,
including Playwright compatibility checks.
"""


def can_playwright_wait(selector: str, selector_type: str) -> bool:
    """Determine if a selector can be used with Playwright's wait_for_selector().

    Playwright's wait_for_selector() only works with selectors that target
    elements. It does not support XPath expressions that return text nodes,
    attributes, or use EXSLT functions.

    Args:
        selector: The selector string.
        selector_type: Type of selector ("xpath" or "css").

    Returns:
        True if Playwright can wait for this selector, False otherwise.

    Examples:
        >>> can_playwright_wait("//div[@class='content']", "xpath")
        True
        >>> can_playwright_wait("//div/@href", "xpath")
        False
        >>> can_playwright_wait("//div/text()", "xpath")
        False
        >>> can_playwright_wait("div.content", "css")
        True
    """
    if selector_type == "css":
        # CSS selectors always target elements
        return True

    # XPath - check for non-element targeting
    selector = selector.strip()

    # Check for text node selection
    if selector.endswith("/text()"):
        return False

    # Check for attribute selection (ends with /@attribute_name)
    if "/@" in selector:
        # Could be selecting an attribute
        # Simple heuristic: if it ends with /@something, it's an attribute
        parts = selector.split("/")
        if parts and parts[-1].startswith("@"):
            return False

    # Check for EXSLT functions (namespace prefixes)
    # Common EXSLT namespaces: re, str, math, set, dyn, exsl, func, date
    exslt_prefixes = [
        "re:",
        "str:",
        "math:",
        "set:",
        "dyn:",
        "exsl:",
        "func:",
        "date:",
    ]

    # Element-targeting XPath if no EXSLT prefixes found
    return all(prefix not in selector for prefix in exslt_prefixes)
