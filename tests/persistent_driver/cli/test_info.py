"""Tests for the info command, output formats, and error handling."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.persistent_driver.cli import cli


class TestInfoCommand:
    """Tests for the info command."""

    def test_info_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test info command with table format."""
        result = runner.invoke(cli, ["info", "--db", str(populated_db)])

        assert result.exit_code == 0
        assert "Run Metadata" in result.output
        assert "test.scraper" in result.output
        assert "Statistics" in result.output
        assert "Queue Total" in result.output

    def test_info_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test info command with JSON format."""
        result = runner.invoke(
            cli, ["info", "--db", str(populated_db), "--format", "json"]
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "metadata" in data
        assert "stats" in data
        assert data["metadata"]["scraper_name"] == "test.scraper"

    def test_info_nonexistent_db(self, runner: CliRunner) -> None:
        """Test info command with non-existent database."""
        result = runner.invoke(cli, ["info", "--db", "/nonexistent/path.db"])

        assert result.exit_code != 0


class TestOutputFormats:
    """Tests for different output formats across commands."""

    def test_table_format_default(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test that table format is the default."""
        result = runner.invoke(
            cli, ["requests", "list", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        # Table format should have column separators
        assert "Total:" in result.output

    def test_json_format(self, runner: CliRunner, populated_db: Path) -> None:
        """Test JSON output format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        # Should be valid JSON
        data = json.loads(result.output)
        assert isinstance(data, dict)

    def test_jsonl_format(self, runner: CliRunner, populated_db: Path) -> None:
        """Test JSONL output format."""
        result = runner.invoke(
            cli,
            [
                "errors",
                "list",
                "--db",
                str(populated_db),
                "--format",
                "jsonl",
                "--limit",
                "10",
            ],
        )

        assert result.exit_code == 0
        # Should be valid JSONL (newline-delimited JSON)
        if result.output.strip():
            lines = result.output.strip().split("\n")
            for line in lines:
                data = json.loads(line)
                assert isinstance(data, dict)


class TestErrorHandling:
    """Tests for error handling and edge cases."""

    def test_invalid_database_path(self, runner: CliRunner) -> None:
        """Test commands with invalid database path."""
        result = runner.invoke(cli, ["info", "--db", "/invalid/path.db"])
        assert result.exit_code != 0

    def test_invalid_format_option(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test commands with invalid format option."""
        result = runner.invoke(
            cli, ["info", "--db", str(populated_db), "--format", "invalid"]
        )
        assert result.exit_code != 0

    def test_missing_required_argument(self, runner: CliRunner) -> None:
        """Test commands with missing required arguments."""
        result = runner.invoke(cli, ["requests", "show"])
        assert result.exit_code != 0

    def test_invalid_request_id(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test commands with invalid request ID."""
        result = runner.invoke(
            cli, ["requests", "show", "--db", str(populated_db), "abc"]
        )
        assert result.exit_code != 0
