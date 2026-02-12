"""Tests for the doctor command and subcommands."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.dev_driver.cli import cli


class TestDoctorCommand:
    """Tests for doctor command and subcommands."""

    def test_doctor_base_command_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test base doctor command with table format."""
        result = runner.invoke(
            cli, ["doctor", "health", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Health Report" in result.output
        assert "Run Status:" in result.output
        assert "Integrity Check:" in result.output
        assert "Errors:" in result.output
        assert "Ghost Requests:" in result.output

    def test_doctor_base_command_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test base doctor command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_base_command_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test base doctor command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_db_on_group(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test --db on the doctor group propagates to subcommands."""
        result = runner.invoke(
            cli, ["doctor", "--db", str(populated_db), "health"]
        )

        assert result.exit_code == 0
        assert "Health Report" in result.output

    def test_doctor_orphans_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor orphans command with table format."""
        result = runner.invoke(
            cli, ["doctor", "orphans", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Orphaned Requests" in result.output
        assert "Orphaned Responses" in result.output

    def test_doctor_orphans_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor orphans command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_orphans_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor orphans command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_pending_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor pending command with table format."""
        result = runner.invoke(
            cli, ["doctor", "pending", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total Pending:" in result.output
        # populated_db has 1 pending request
        assert "1" in result.output

    def test_doctor_pending_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor pending command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_pending_with_limit(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor pending command with limit option."""
        result = runner.invoke(
            cli,
            ["doctor", "pending", "--db", str(populated_db), "--limit", "50"],
        )

        assert result.exit_code == 0

    def test_doctor_ghosts_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor ghosts command with table format."""
        result = runner.invoke(
            cli, ["doctor", "ghosts", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Ghost Requests" in result.output
        assert "Total:" in result.output

    def test_doctor_ghosts_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor ghosts command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_ghosts_jsonl_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor ghosts command with JSONL format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
                "ghosts",
                "--db",
                str(populated_db),
                "--format",
                "jsonl",
            ],
        )

        assert result.exit_code == 0
        # Output should be valid (may be empty if no ghosts)

    def test_doctor_ghosts_with_continuation_filter(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor ghosts command with continuation filter."""
        result = runner.invoke(
            cli,
            [
                "doctor",
                "ghosts",
                "--db",
                str(populated_db),
                "--continuation",
                "step1",
            ],
        )

        assert result.exit_code == 0

    def test_doctor_ghosts_nonexistent_continuation(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor ghosts with nonexistent continuation."""
        result = runner.invoke(
            cli,
            [
                "doctor",
                "ghosts",
                "--db",
                str(populated_db),
                "--continuation",
                "nonexistent",
            ],
        )

        assert result.exit_code == 0
        assert "No ghost requests found" in result.output

    def test_doctor_health_includes_estimates(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test that doctor health includes estimates section."""
        result = runner.invoke(
            cli, ["doctor", "health", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Estimates:" in result.output

    def test_doctor_health_json_includes_estimates(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test that doctor health JSON includes estimates."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_estimates_table_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor estimates command with table format."""
        result = runner.invoke(
            cli, ["doctor", "estimates", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Estimate Checks" in result.output

    def test_doctor_estimates_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor estimates command with JSON format."""
        result = runner.invoke(
            cli,
            [
                "doctor",
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

    def test_doctor_estimates_failures_only(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test doctor estimates command with --failures-only flag."""
        result = runner.invoke(
            cli,
            [
                "doctor",
                "estimates",
                "--db",
                str(populated_db),
                "--failures-only",
            ],
        )

        assert result.exit_code == 0
