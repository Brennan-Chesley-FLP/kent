"""Tests for the errors and results commands."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from kent.driver.dev_driver.cli import cli


class TestErrorsCommands:
    """Tests for the errors commands."""

    def test_errors_list(self, runner: CliRunner, populated_db: Path) -> None:
        """Test errors list command."""
        result = runner.invoke(
            cli, ["errors", "list", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total: 2" in result.output

    def test_errors_list_filter_by_type(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering errors by type."""
        result = runner.invoke(
            cli,
            ["errors", "list", "--db", str(populated_db), "--type", "xpath"],
        )

        assert result.exit_code == 0
        assert "Total: 1" in result.output

    def test_errors_list_filter_by_resolution(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering errors by resolution status."""
        result = runner.invoke(
            cli, ["errors", "list", "--db", str(populated_db), "--unresolved"]
        )

        assert result.exit_code == 0
        assert "Total: 1" in result.output

    def test_errors_show(self, runner: CliRunner, populated_db: Path) -> None:
        """Test errors show command."""
        result = runner.invoke(
            cli, ["errors", "show", "--db", str(populated_db), "1"]
        )

        assert result.exit_code == 0
        assert "ID: 1" in result.output
        assert "Type:" in result.output
        assert "xpath" in result.output

    def test_errors_summary(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test errors summary command."""
        result = runner.invoke(
            cli, ["errors", "summary", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Totals" in result.output or "By Type" in result.output

    def test_errors_resolve(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test errors resolve command."""
        result = runner.invoke(
            cli,
            [
                "errors",
                "resolve",
                "--db",
                str(populated_db),
                "1",
                "--notes",
                "Fixed",
            ],
        )

        assert result.exit_code == 0
        assert "resolved" in result.output

    def test_errors_requeue(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test errors requeue command."""
        result = runner.invoke(
            cli,
            [
                "errors",
                "requeue",
                "--db",
                str(populated_db),
                "1",
                "--notes",
                "Trying again",
            ],
        )

        assert result.exit_code == 0
        assert "requeued" in result.output


class TestResultsCommands:
    """Tests for the results commands."""

    def test_results_list(self, runner: CliRunner, populated_db: Path) -> None:
        """Test results list command."""
        result = runner.invoke(
            cli, ["results", "list", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "Total: 2" in result.output

    def test_results_list_filter_by_validity(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test filtering results by validity."""
        result = runner.invoke(
            cli, ["results", "list", "--db", str(populated_db), "--valid"]
        )

        assert result.exit_code == 0
        assert "Total: 1" in result.output

    def test_results_show(self, runner: CliRunner, populated_db: Path) -> None:
        """Test results show command."""
        result = runner.invoke(
            cli, ["results", "show", "--db", str(populated_db), "1"]
        )

        assert result.exit_code == 0
        assert "ID: 1" in result.output
        assert "Type:" in result.output

    def test_results_summary(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test results summary command."""
        result = runner.invoke(
            cli, ["results", "summary", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert "TestResult" in result.output
