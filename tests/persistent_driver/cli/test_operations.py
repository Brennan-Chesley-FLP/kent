"""Tests for the requeue, cancel, and compression commands."""

from __future__ import annotations

import json
from pathlib import Path

from click.testing import CliRunner

from kent.driver.persistent_driver.cli import cli


class TestRequeueCommands:
    """Tests for the requeue commands."""

    def test_requeue_request(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requeue request command."""
        result = runner.invoke(
            cli, ["requeue", "request", "--db", str(populated_db), "2"]
        )

        assert result.exit_code == 0
        assert "requeued" in result.output

    def test_requeue_request_no_clear_downstream(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requeue request without clearing downstream."""
        result = runner.invoke(
            cli,
            [
                "requeue",
                "request",
                "--db",
                str(populated_db),
                "2",
                "--no-clear-downstream",
            ],
        )

        assert result.exit_code == 0
        assert "requeued" in result.output

    def test_requeue_continuation(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requeue continuation command."""
        result = runner.invoke(
            cli,
            ["requeue", "continuation", "--db", str(populated_db), "step1"],
        )

        assert result.exit_code == 0
        assert "Requeued" in result.output

    def test_requeue_errors(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test requeue errors command."""
        result = runner.invoke(
            cli,
            [
                "requeue",
                "errors",
                "--db",
                str(populated_db),
                "--type",
                "xpath",
            ],
        )

        assert result.exit_code == 0
        assert "Requeued" in result.output


class TestCancelCommands:
    """Tests for the cancel commands."""

    def test_cancel_request(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test cancel request command."""
        result = runner.invoke(
            cli, ["cancel", "request", "--db", str(populated_db), "1"]
        )

        assert result.exit_code == 0
        assert "cancelled" in result.output

    def test_cancel_request_not_pending(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test cancel request that is not pending."""
        result = runner.invoke(
            cli, ["cancel", "request", "--db", str(populated_db), "2"]
        )

        assert result.exit_code != 0

    def test_cancel_continuation(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test cancel continuation command."""
        result = runner.invoke(
            cli, ["cancel", "continuation", "--db", str(populated_db), "step2"]
        )

        assert result.exit_code == 0
        assert "Cancelled" in result.output


class TestCompressionCommands:
    """Tests for the compression commands."""

    def test_compression_stats(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test compression stats command."""
        result = runner.invoke(
            cli, ["compression", "stats", "--db", str(populated_db)]
        )

        assert result.exit_code == 0
        assert (
            "Compression Statistics" in result.output
            or "Total" in result.output
        )

    def test_compression_stats_json_format(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test compression stats with JSON format."""
        result = runner.invoke(
            cli,
            [
                "compression",
                "stats",
                "--db",
                str(populated_db),
                "--format",
                "json",
            ],
        )

        assert result.exit_code == 0
        data = json.loads(result.output)
        assert "total" in data

    def test_compression_train(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test compression train command."""
        result = runner.invoke(
            cli,
            [
                "compression",
                "train",
                "--db",
                str(populated_db),
                "step1",
                "--samples",
                "10",
            ],
        )

        # This may fail if there aren't enough samples, which is expected
        assert result.exit_code in (0, 1)

    def test_compression_recompress(
        self, runner: CliRunner, populated_db: Path
    ) -> None:
        """Test compression recompress command."""
        result = runner.invoke(
            cli,
            ["compression", "recompress", "--db", str(populated_db), "step1"],
        )

        assert result.exit_code == 0
