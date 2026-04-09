"""Tests for Playwright wait condition dataclasses."""

import pytest

from kent.data_types import (
    WaitForLoadState,
    WaitForSelector,
    WaitForTimeout,
    WaitForURL,
)


class TestWaitForSelector:
    """Tests for WaitForSelector wait condition."""

    def test_basic_construction(self):
        """Test basic WaitForSelector construction with selector only."""
        wait = WaitForSelector(selector="//div[@id='content']")
        assert wait.selector == "//div[@id='content']"
        assert wait.state == "visible"
        assert wait.timeout is None

    def test_construction_with_state(self):
        """Test WaitForSelector construction with custom state."""
        wait = WaitForSelector(selector=".loading-spinner", state="hidden")
        assert wait.selector == ".loading-spinner"
        assert wait.state == "hidden"
        assert wait.timeout is None

    def test_construction_with_timeout(self):
        """Test WaitForSelector construction with custom timeout."""
        wait = WaitForSelector(
            selector="#submit-button", state="attached", timeout=5000
        )
        assert wait.selector == "#submit-button"
        assert wait.state == "attached"
        assert wait.timeout == 5000

    def test_frozen_dataclass(self):
        """Test that WaitForSelector is frozen and immutable."""
        wait = WaitForSelector(selector="//button")
        with pytest.raises(AttributeError):
            wait.selector = "//div"

    def test_valid_states(self):
        """Test construction with all valid states."""
        states = ["attached", "detached", "visible", "hidden"]
        for state in states:
            wait = WaitForSelector(selector="#element", state=state)
            assert wait.state == state


class TestWaitForLoadState:
    """Tests for WaitForLoadState wait condition."""

    def test_basic_construction(self):
        """Test basic WaitForLoadState construction with default state."""
        wait = WaitForLoadState()
        assert wait.state == "load"
        assert wait.timeout is None

    def test_construction_with_state(self):
        """Test WaitForLoadState construction with custom state."""
        wait = WaitForLoadState(state="networkidle")
        assert wait.state == "networkidle"
        assert wait.timeout is None

    def test_construction_with_timeout(self):
        """Test WaitForLoadState construction with custom timeout."""
        wait = WaitForLoadState(state="domcontentloaded", timeout=10000)
        assert wait.state == "domcontentloaded"
        assert wait.timeout == 10000

    def test_frozen_dataclass(self):
        """Test that WaitForLoadState is frozen and immutable."""
        wait = WaitForLoadState(state="load")
        with pytest.raises(AttributeError):
            wait.state = "networkidle"

    def test_valid_states(self):
        """Test construction with all valid load states."""
        states = ["load", "domcontentloaded", "networkidle"]
        for state in states:
            wait = WaitForLoadState(state=state)
            assert wait.state == state


class TestWaitForURL:
    """Tests for WaitForURL wait condition."""

    def test_basic_construction(self):
        """Test basic WaitForURL construction with URL string."""
        wait = WaitForURL(url="https://example.com/results")
        assert wait.url == "https://example.com/results"
        assert wait.timeout is None

    def test_construction_with_timeout(self):
        """Test WaitForURL construction with custom timeout."""
        wait = WaitForURL(
            url="https://example.com/search?q=test", timeout=8000
        )
        assert wait.url == "https://example.com/search?q=test"
        assert wait.timeout == 8000

    def test_frozen_dataclass(self):
        """Test that WaitForURL is frozen and immutable."""
        wait = WaitForURL(url="https://example.com")
        with pytest.raises(AttributeError):
            wait.url = "https://other.com"

    def test_construction_with_pattern(self):
        """Test WaitForURL construction with regex pattern."""
        wait = WaitForURL(url=r".*\/results\/\d+")
        assert wait.url == r".*\/results\/\d+"
        assert wait.timeout is None


class TestWaitForTimeout:
    """Tests for WaitForTimeout wait condition."""

    def test_basic_construction(self):
        """Test basic WaitForTimeout construction."""
        wait = WaitForTimeout(timeout=1000)
        assert wait.timeout == 1000

    def test_construction_with_various_timeouts(self):
        """Test WaitForTimeout construction with various timeout values."""
        timeouts = [100, 500, 1000, 5000, 30000]
        for timeout in timeouts:
            wait = WaitForTimeout(timeout=timeout)
            assert wait.timeout == timeout

    def test_frozen_dataclass(self):
        """Test that WaitForTimeout is frozen and immutable."""
        wait = WaitForTimeout(timeout=2000)
        with pytest.raises(AttributeError):
            wait.timeout = 3000

    def test_zero_timeout(self):
        """Test WaitForTimeout construction with zero timeout."""
        wait = WaitForTimeout(timeout=0)
        assert wait.timeout == 0


class TestWaitConditionsCombination:
    """Tests for using multiple wait conditions together."""

    def test_await_list_simulation(self):
        """Test creating a list of wait conditions as would be used in await_list."""
        await_list = [
            WaitForLoadState(state="domcontentloaded"),
            WaitForSelector(selector="#results-table", state="visible"),
            WaitForTimeout(timeout=500),
        ]

        assert len(await_list) == 3
        assert isinstance(await_list[0], WaitForLoadState)
        assert isinstance(await_list[1], WaitForSelector)
        assert isinstance(await_list[2], WaitForTimeout)

    def test_mixed_wait_conditions_with_timeouts(self):
        """Test creating wait conditions with various timeout configurations."""
        await_list = [
            WaitForURL(url="https://example.com/search", timeout=5000),
            WaitForSelector(
                selector=".search-results", state="visible", timeout=10000
            ),
            WaitForLoadState(state="networkidle", timeout=15000),
        ]

        assert await_list[0].timeout == 5000
        assert await_list[1].timeout == 10000
        assert await_list[2].timeout == 15000
