"""Tests for the ``pdd query`` subcommand."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

import pytest
from click.testing import CliRunner
from pydantic import ValidationError

from kent.driver.persistent_driver.cli import cli
from kent.driver.persistent_driver.cli import query as query_mod
from kent.driver.persistent_driver.database import SCHEMA_VERSION


@pytest.fixture
def isolated_user_queries(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Path:
    """Point USER_QUERIES_DIR at a fresh tmp dir for each test."""
    d = tmp_path / "user_queries"
    d.mkdir()
    monkeypatch.setattr(query_mod, "USER_QUERIES_DIR", d)
    return d


def _write_query(directory: Path, name: str, **overrides: Any) -> Path:
    body = {
        "schema_version": SCHEMA_VERSION,
        "description": "Test query.",
        "query": "SELECT count(*) AS n FROM requests;",
        "params": [],
    }
    body.update(overrides)
    path = directory / f"{name}.json"
    path.write_text(json.dumps(body))
    return path


# =========================================================================
# Pydantic model
# =========================================================================


class TestQueryDef:
    def test_accepts_well_formed(self) -> None:
        qdef = query_mod.QueryDef.model_validate_json(
            json.dumps(
                {
                    "schema_version": 19,
                    "description": "x",
                    "query": "SELECT 1;",
                    "params": [],
                }
            )
        )
        assert qdef.schema_version == 19

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            query_mod.QueryDef.model_validate_json(
                json.dumps(
                    {
                        "schema_version": 19,
                        "description": "x",
                        "query": "SELECT 1;",
                        "params": [],
                        "unexpected": "nope",
                    }
                )
            )

    def test_placeholder_mismatch_rejected(self) -> None:
        with pytest.raises(ValidationError):
            query_mod.QueryDef.model_validate_json(
                json.dumps(
                    {
                        "schema_version": 19,
                        "description": "x",
                        "query": "SELECT * FROM r WHERE a = :b",
                        "params": ["a"],
                    }
                )
            )


# =========================================================================
# query list
# =========================================================================


def test_query_list_shows_builtins(
    runner: CliRunner, isolated_user_queries: Path
) -> None:
    result = runner.invoke(cli, ["query", "list"])
    assert result.exit_code == 0, result.output
    assert "requests_by_status" in result.output
    assert "requests_for_step" in result.output


def test_query_list_json_format(
    runner: CliRunner, isolated_user_queries: Path
) -> None:
    result = runner.invoke(cli, ["query", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    names = {item["name"] for item in data["items"]}
    assert "requests_by_status" in names
    for item in data["items"]:
        assert "description" in item
        assert "schema_version" in item
        assert "params" in item
        assert "source" in item


def test_query_list_user_overrides_builtin(
    runner: CliRunner, isolated_user_queries: Path
) -> None:
    _write_query(
        isolated_user_queries,
        "requests_by_status",
        description="USER OVERRIDE",
    )
    result = runner.invoke(cli, ["query", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    items = json.loads(result.output)["items"]
    matches = [i for i in items if i["name"] == "requests_by_status"]
    assert len(matches) == 1
    assert matches[0]["source"] == "user"
    assert matches[0]["description"] == "USER OVERRIDE"


def test_query_list_skips_reserved_name(
    isolated_user_queries: Path,
) -> None:
    (isolated_user_queries / "list.json").write_text(
        json.dumps(
            {
                "schema_version": SCHEMA_VERSION,
                "description": "should be ignored",
                "query": "SELECT 1",
                "params": [],
            }
        )
    )
    result = CliRunner().invoke(cli, ["query", "list", "--format", "json"])
    assert result.exit_code == 0, result.output
    items = json.loads(result.stdout)["items"]
    names = {i["name"] for i in items}
    assert "list" not in names
    assert "reserved" in result.stderr.lower()


# =========================================================================
# query run
# =========================================================================


def test_named_query_resolves_builtin(
    runner: CliRunner,
    populated_db: Path,
    isolated_user_queries: Path,
) -> None:
    result = runner.invoke(
        cli,
        [
            "query",
            "requests_by_status",
            "--db",
            str(populated_db),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["name"] == "requests_by_status"
    assert data["row_count"] > 0


def test_implicit_run_dispatch(
    runner: CliRunner,
    populated_db: Path,
    isolated_user_queries: Path,
) -> None:
    # No explicit 'run' subcommand — QueryGroup should route through it.
    result = runner.invoke(
        cli,
        [
            "query",
            "requests_by_status",
            "--db",
            str(populated_db),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output


def test_ad_hoc_query_path(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(tmp_path, "adhoc")
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["name"] == "adhoc"
    assert data["columns"] == ["n"]


def test_query_params_required_missing(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(
        tmp_path,
        "needs_step",
        query="SELECT * FROM requests WHERE continuation = :step;",
        params=["step"],
    )
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
        ],
    )
    assert result.exit_code != 0
    assert "missing" in result.output.lower()
    assert "step" in result.output


def test_query_params_extra_rejected(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(tmp_path, "no_params")
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
            "--query-params",
            '{"surprise": 1}',
        ],
    )
    assert result.exit_code != 0
    assert "unexpected" in result.output.lower()
    assert "surprise" in result.output


def test_query_params_not_json_object(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(tmp_path, "no_params")
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
            "--query-params",
            "[1, 2]",
        ],
    )
    assert result.exit_code != 0


def test_schema_mismatch_aborts(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(tmp_path, "wrong_ver", schema_version=9999)
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
        ],
    )
    assert result.exit_code != 0
    assert "9999" in result.output
    assert "--force" in result.output


def test_schema_mismatch_force_overrides(
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(tmp_path, "wrong_ver", schema_version=9999)
    result = CliRunner().invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
            "--force",
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "warning" in result.stderr.lower()


def test_read_only_enforced(
    runner: CliRunner,
    populated_db: Path,
    tmp_path: Path,
    isolated_user_queries: Path,
) -> None:
    qpath = _write_query(
        tmp_path,
        "evil",
        query=(
            "INSERT INTO requests (method, url, continuation, status, "
            "priority, request_type, headers_json, cookies_json, "
            "accumulated_data_json, permanent_json, current_location, "
            "queue_counter, retry_count, created_at) VALUES "
            "('GET', 'x', 'x', 'pending', 1, 'navigating', '{}', '{}', "
            "'{}', '{}', '', 999, 0, datetime('now'));"
        ),
        params=[],
    )
    result = runner.invoke(
        cli,
        [
            "query",
            "--query",
            str(qpath),
            "--db",
            str(populated_db),
        ],
    )
    assert result.exit_code != 0
    # Verify DB was not modified, using a plain sqlite3 connection
    # (sync — this test is not async).
    conn = sqlite3.connect(str(populated_db))
    try:
        count = conn.execute("SELECT count(*) FROM requests;").fetchone()[0]
    finally:
        conn.close()
    assert count == 5  # populated_db has exactly 5 requests


def test_format_json(
    runner: CliRunner,
    populated_db: Path,
    isolated_user_queries: Path,
) -> None:
    result = runner.invoke(
        cli,
        [
            "query",
            "requests_by_status",
            "--db",
            str(populated_db),
            "--format",
            "json",
        ],
    )
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert "columns" in data and "rows" in data


def test_format_jsonl(
    runner: CliRunner,
    populated_db: Path,
    isolated_user_queries: Path,
) -> None:
    # Ad-hoc query that returns a list so jsonl emits per-row lines.
    # For our shape, jsonl emits data["items"] if present — but our
    # output dict doesn't use "items". So jsonl emits the whole dict
    # as one line. That's acceptable current behavior; just assert
    # it parses.
    result = runner.invoke(
        cli,
        [
            "query",
            "requests_by_status",
            "--db",
            str(populated_db),
            "--format",
            "jsonl",
        ],
    )
    assert result.exit_code == 0, result.output
    for line in result.output.strip().splitlines():
        json.loads(line)


def test_format_default(
    runner: CliRunner,
    populated_db: Path,
    isolated_user_queries: Path,
) -> None:
    result = runner.invoke(
        cli,
        [
            "query",
            "requests_by_status",
            "--db",
            str(populated_db),
        ],
    )
    assert result.exit_code == 0, result.output
    assert "requests_by_status" in result.output
    assert "continuation" in result.output  # a column name


# =========================================================================
# --help
# =========================================================================


def test_help_shows_query_metadata_named(
    runner: CliRunner, isolated_user_queries: Path
) -> None:
    result = runner.invoke(cli, ["query", "requests_for_step", "--help"])
    assert result.exit_code == 0, result.output
    assert "requests_for_step" in result.output
    # Description and required params should be printed
    assert "continuation" in result.output or "step" in result.output
    assert "step" in result.output


def test_help_shows_query_metadata_adhoc(
    runner: CliRunner, tmp_path: Path, isolated_user_queries: Path
) -> None:
    qpath = _write_query(
        tmp_path,
        "adhoc",
        description="unique-adhoc-description-xyz",
        query="SELECT * FROM r WHERE a = :foo",
        params=["foo"],
    )
    result = runner.invoke(cli, ["query", "--query", str(qpath), "--help"])
    assert result.exit_code == 0, result.output
    assert "unique-adhoc-description-xyz" in result.output
    assert "foo" in result.output


def test_help_bare_no_query_metadata_leak(
    runner: CliRunner, isolated_user_queries: Path
) -> None:
    result = runner.invoke(cli, ["query", "--help"])
    assert result.exit_code == 0, result.output
    # Should not include per-query description fields
    assert "Required params:" not in result.output


def test_help_on_malformed_query_does_not_crash(
    runner: CliRunner, tmp_path: Path, isolated_user_queries: Path
) -> None:
    bad = tmp_path / "bad.json"
    bad.write_text("{not: valid json}")
    result = runner.invoke(cli, ["query", "--query", str(bad), "--help"])
    assert result.exit_code == 0
    # Baseline help is still present
    assert "query" in result.output.lower()
