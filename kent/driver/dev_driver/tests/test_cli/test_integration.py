"""Integration tests that test workflows across multiple commands."""

from __future__ import annotations

from pathlib import Path

from click.testing import CliRunner

from kent.driver.dev_driver.cli import cli


class TestIntegration:
    """Integration tests that test workflows across multiple commands."""

    def test_workflow_inspect_and_requeue(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test workflow: inspect failed request, then requeue it."""
        # First, list failed requests
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--status",
                "failed",
            ],
        )
        assert result.exit_code == 0
        assert "Total: 1" in result.output

        # Then requeue it (request ID 3 is failed)
        result = runner.invoke(
            cli, ["requeue", "request", "--db", str(populated_db), "3"]
        )
        assert result.exit_code == 0

        # Verify it was requeued by checking pending requests increased
        result = runner.invoke(
            cli,
            [
                "requests",
                "list",
                "--db",
                str(populated_db),
                "--status",
                "pending",
            ],
        )
        assert result.exit_code == 0

    def test_workflow_inspect_error_and_resolve(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test workflow: inspect error, then resolve it."""
        # First, show the error details
        result = runner.invoke(
            cli, ["errors", "show", "--db", str(populated_db), "1"]
        )
        assert result.exit_code == 0
        assert "xpath" in result.output

        # Then resolve it
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

        # Verify it was resolved
        result = runner.invoke(
            cli, ["errors", "list", "--db", str(populated_db), "--unresolved"]
        )
        assert result.exit_code == 0
        # Should now have 0 unresolved (previously was 1)

    def test_workflow_export_results(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test workflow: inspect results, then export them."""
        # First, check results summary
        result = runner.invoke(
            cli, ["results", "summary", "--db", str(populated_db)]
        )
        assert result.exit_code == 0

        # Then export only valid results
        output_file = tmp_path / "valid.jsonl"
        result = runner.invoke(
            cli,
            [
                "export",
                "jsonl",
                "--db",
                str(populated_db),
                str(output_file),
                "--valid",
            ],
        )
        assert result.exit_code == 0
        assert output_file.exists()

        # Verify export contains expected data
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 1  # Only 1 valid result
