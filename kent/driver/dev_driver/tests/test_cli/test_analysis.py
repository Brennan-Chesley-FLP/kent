"""Tests for the diagnose and export commands."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.dev_driver.cli import cli


class TestDiagnoseCommand:
    """Tests for the diagnose command."""

    def test_diagnose_error_without_response(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test diagnose command on error without response."""
        result = runner.invoke(
            cli, ["diagnose", "--db", str(populated_db), "1"]
        )

        # Should fail because error 1 doesn't have a response
        assert result.exit_code != 0

    def test_diagnose_error_not_found(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test diagnose command on non-existent error."""
        result = runner.invoke(
            cli, ["diagnose", "--db", str(populated_db), "9999"]
        )

        assert result.exit_code != 0
        assert "not found" in result.output


class TestExportCommands:
    """Tests for the export commands."""

    def test_export_jsonl(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test export jsonl command."""
        output_file = tmp_path / "results.jsonl"
        result = runner.invoke(
            cli,
            ["export", "jsonl", "--db", str(populated_db), str(output_file)],
        )

        assert result.exit_code == 0
        assert "Exported" in result.output
        assert output_file.exists()

        # Verify content
        lines = output_file.read_text().strip().split("\n")
        assert len(lines) == 2
        first_result = json.loads(lines[0])
        assert "id" in first_result
        assert "result_type" in first_result

    def test_export_jsonl_filtered(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test export jsonl with filters."""
        output_file = tmp_path / "valid_results.jsonl"
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
        assert "Exported 1" in result.output

    def test_export_warc(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test export warc command."""
        output_file = tmp_path / "archive.warc.gz"
        result = runner.invoke(
            cli,
            ["export", "warc", "--db", str(populated_db), str(output_file)],
        )

        assert result.exit_code == 0
        assert "Exported" in result.output
        assert output_file.exists()

    def test_export_warc_no_compress(
        self, runner: CliRunner, populated_db: Path, tmp_path: Path
    ) -> None:
        """Test export warc without compression."""
        output_file = tmp_path / "archive.warc"
        result = runner.invoke(
            cli,
            [
                "export",
                "warc",
                "--db",
                str(populated_db),
                str(output_file),
                "--no-compress",
            ],
        )

        assert result.exit_code == 0
        assert "Exported" in result.output
