"""Tests for health check and diagnostic commands (scrape/requests)."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.persistent_driver.cli import cli


class TestHealthCommands:
    """Tests for scrape health and related commands."""

    def test_scrape_health_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape health command with table format."""
        result = runner.invoke(
            cli, ["scrape", "health", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Health Report" in result.output
        assert "Run Status:" in result.output
        assert "Integrity Check:" in result.output
        assert "Errors:" in result.output
        assert "Ghost Requests:" in result.output

    def test_scrape_health_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape health command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "scrape",
                "health",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "status" in data
        assert "integrity" in data
        assert "ghosts" in data
        assert "error_stats" in data

    def test_scrape_health_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape health command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "scrape",
                "health",
                "--db",
                str(populated_db),
                "--format",
                "jsonl",
            ],
        )

        assert result.exit_code == 0
        lines = result.output.strip().split("\n")
        assert len(lines) == 5  # status, integrity, ghosts, errors, estimates

        # Each line should be valid JSON with section field
        for line in lines:
            data = json.loads(line)
            assert "section" in data

    def test_scrape_db_on_group(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test --db on the scrape group propagates to subcommands."""
        result = runner.invoke(
            cli, ["scrape", "--db", str(populated_db), "health"]
        )

        assert result.exit_code == 0
        assert "Health Report" in result.output

    def test_requests_orphans_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests orphans command with table format."""
        result = runner.invoke(
            cli, ["requests", "orphans", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Orphaned Requests" in result.output
        assert "Orphaned Responses" in result.output

    def test_requests_orphans_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests orphans command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "orphans",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "orphaned_requests" in data
        assert "orphaned_responses" in data
        assert isinstance(data["orphaned_requests"], list)
        assert isinstance(data["orphaned_responses"], list)

    def test_requests_orphans_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests orphans command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "orphans",
                "--db",
                str(populated_db),
                "--format",
                "jsonl",
            ],
        )

        assert result.exit_code == 0
        # Should have lines for any orphaned requests/responses
        # In populated_db, we have no orphans, so output may be empty or minimal

    def test_requests_pending_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests pending command with table format."""
        result = runner.invoke(
            cli, ["requests", "pending", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total Pending:" in result.output
        # populated_db has 1 pending request
        assert "1" in result.output

    def test_requests_pending_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests pending command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "pending",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total" in data
        assert "items" in data
        assert data["total"] == 1  # One pending request in populated_db

    def test_requests_pending_with_limit(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests pending command with limit option."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "pending",
                "--db",
                str(populated_db),
                "--limit",
                "50",
            ],
        )

        assert result.exit_code == 0

    def test_requests_ghosts_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests ghosts command with table format."""
        result = runner.invoke(
            cli, ["requests", "ghosts", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Ghost Requests" in result.output
        assert "Total:" in result.output

    def test_requests_ghosts_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests ghosts command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "ghosts",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total_count" in data
        assert "by_continuation" in data
        assert "ghosts" in data

    def test_requests_ghosts_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests ghosts command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "ghosts",
                "--db",
                str(populated_db),
                "--format",
                "jsonl",
            ],
        )

        assert result.exit_code == 0
        # Output should be valid (may be empty if no ghosts)

    def test_requests_ghosts_with_step_filter(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests ghosts command with step filter."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "ghosts",
                "--db",
                str(populated_db),
                "--step",
                "step1",
            ],
        )

        assert result.exit_code == 0

    def test_requests_ghosts_nonexistent_step(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requests ghosts with nonexistent step."""
        result = runner.invoke(
            cli,
            [
                "requests",
                "ghosts",
                "--db",
                str(populated_db),
                "--step",
                "nonexistent",
            ],
        )

        assert result.exit_code == 0
        assert "No ghost requests found" in result.output

    def test_scrape_health_includes_estimates(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test that scrape health includes estimates section."""
        result = runner.invoke(
            cli, ["scrape", "health", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Estimates:" in result.output

    def test_scrape_health_json_includes_estimates(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test that scrape health JSON includes estimates."""
        result = runner.invoke(
            cli,
            [
                "scrape",
                "health",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "estimates" in data

    def test_scrape_estimates_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape estimates command with table format."""
        result = runner.invoke(
            cli, ["scrape", "estimates", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Estimate Checks" in result.output

    def test_scrape_estimates_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape estimates command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "scrape",
                "estimates",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "estimates" in data
        assert "summary" in data

    def test_scrape_estimates_failures_only(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test scrape estimates command with --failures-only flag."""
        result = runner.invoke(
            cli,
            [
                "scrape",
                "estimates",
                "--db",
                str(populated_db),
                "--failures-only",
            ],
        )

        assert result.exit_code == 0
