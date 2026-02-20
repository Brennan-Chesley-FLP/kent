"""Tests for the requests and responses commands."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.persistent_driver.cli import cli


class TestRequestsCommands:
    """Tests for the requests commands."""

    def test_requests_list(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests list command."""
        result = runner.invoke(
            cli, ["requests", "list", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total: 5" in result.output
        assert (
            "https://example" in result.output
        )  # URLs are truncated in table format

    def test_requests_list_filter_by_status(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering requests by status."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--status",
                "completed",
            ],
        )

        assert result.exit_code == 0
        assert "Total: 2" in result.output

    def test_requests_list_filter_by_continuation(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering requests by continuation."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--continuation",
                "step1",
            ],
        )

        assert result.exit_code == 0
        assert "Total: 3" in result.output

    def test_requests_list_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests list with JSON format."""
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
        data = json.loads(result.output)
        assert "items" in data
        assert "total" in data
        assert data["total"] == 5

    def test_requests_list_pagination(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests list with pagination."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--limit",
                "2",
                "--offset",
                "0",
            ],
        )

        assert result.exit_code == 0
        assert "Showing: 2" in result.output
        assert "Limit: 2" in result.output

    def test_requests_show(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests show command."""
        result = runner.invoke(
            cli, ["requests", "show", "--db", str(populated_db), "1"]
        )

        assert result.exit_code == 0
        assert "ID: 1" in result.output
        assert "example.com/page1" in result.output
        assert "Status:" in result.output

    def test_requests_show_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests show with JSON format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "show",
                "--db",
                str(populated_db),
                "1",
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert data["id"] == 1
        assert "url" in data

    def test_requests_show_not_found(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests show with non-existent request."""
        result = runner.invoke(
            cli, ["requests", "show", "--db", str(populated_db), "9999"]
        )

        assert result.exit_code != 0
        assert "not found" in result.output

    def test_requests_summary(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests summary command."""
        result = runner.invoke(
            cli, ["requests", "summary", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "step1" in result.output or "step2" in result.output


class TestResponsesCommands:
    """Tests for the responses commands."""

    def test_responses_list(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test responses list command."""
        result = runner.invoke(
            cli, ["responses", "list", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total: 2" in result.output

    def test_responses_list_filter_by_continuation(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering responses by continuation."""
        result = runner.invoke(
            cli,
            [
                "responses",
                "list",
                "--db",
                str(populated_db),
                "--continuation",
                "step1",
            ],
        )

        assert result.exit_code == 0
        assert "Total: 2" in result.output

    def test_responses_show(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test responses show command."""
        result = runner.invoke(
            cli, ["responses", "show", "--db", str(populated_db), "2"]
        )

        assert result.exit_code == 0
        assert "ID: 2" in result.output
        assert "Status Code:" in result.output

    def test_responses_content(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test responses content command."""
        result = runner.invoke(
            cli, ["responses", "content", "--db", str(populated_db), "2"]
        )

        assert result.exit_code == 0
        assert "Response 1" in result.output

    def test_responses_content_to_file(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test responses content command with output file."""
        output_file = tmp_path / "response.html"
        result = runner.invoke(
            cli,
            [
                "responses",
                "content",
                "--db",
                str(populated_db),
                "2",
                "-o",
                str(output_file),
            ],
        )

        assert result.exit_code == 0
        assert output_file.exists()
        assert b"Response 1" in output_file.read_bytes()
