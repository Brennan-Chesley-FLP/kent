"""Tests for web interface: run manager, FastAPI app, REST API, WebSocket."""

from __future__ import annotations

from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa


class TestRunManager:
    """Tests for RunManager class."""

    @pytest.fixture
    def runs_dir(self, tmp_path: Path) -> Path:
        """Create a temporary runs directory."""
        runs = tmp_path / "runs"
        runs.mkdir()
        return runs

    @pytest.fixture
    def mock_scraper(self) -> Any:
        """Create a minimal mock scraper for testing."""

        from kent.data_types import (
            BaseScraper,
            HttpMethod,
            HTTPRequestParams,
            Request,
        )

        class MockScraper(BaseScraper[str]):
            def get_entry(self) -> Generator[Request, None, None]:
                yield Request(
                    request=HTTPRequestParams(
                        method=HttpMethod.GET,
                        url="https://example.com",
                    ),
                    continuation="parse",
                    current_location="",
                )

            def parse(self, response: Any) -> list:
                return []

        return MockScraper()

    async def test_scan_runs_empty_dir(self, runs_dir: Path) -> None:
        """Test scanning empty runs directory."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)
        discovered = await manager.scan_runs()

        assert discovered == []
        assert manager.runs == {}

    async def test_scan_runs_creates_missing_dir(self, tmp_path: Path) -> None:
        """Test that scan_runs creates the directory if it doesn't exist."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        nonexistent = tmp_path / "nonexistent_runs"
        assert not nonexistent.exists()

        manager = RunManager(nonexistent)
        await manager.scan_runs()

        assert nonexistent.exists()

    async def test_scan_runs_discovers_databases(self, runs_dir: Path) -> None:
        """Test scanning directory with existing database files."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        # Create some .db files
        (runs_dir / "run1.db").touch()
        (runs_dir / "run2.db").touch()
        (runs_dir / "notadb.txt").touch()  # Should be ignored

        manager = RunManager(runs_dir)
        discovered = await manager.scan_runs()

        assert len(discovered) == 2
        assert "run1" in discovered
        assert "run2" in discovered
        assert "notadb" not in discovered
        assert len(manager.runs) == 2
        assert all(r.status == "unloaded" for r in manager.runs.values())

    async def test_list_runs(self, runs_dir: Path) -> None:
        """Test listing all runs."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        (runs_dir / "test1.db").touch()
        (runs_dir / "test2.db").touch()

        manager = RunManager(runs_dir)
        await manager.scan_runs()

        runs = await manager.list_runs()
        assert len(runs) == 2
        run_ids = {r.run_id for r in runs}
        assert run_ids == {"test1", "test2"}

    async def test_get_run_found(self, runs_dir: Path) -> None:
        """Test getting a specific run."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        (runs_dir / "myrun.db").touch()

        manager = RunManager(runs_dir)
        await manager.scan_runs()

        run = await manager.get_run("myrun")
        assert run is not None
        assert run.run_id == "myrun"

    async def test_get_run_not_found(self, runs_dir: Path) -> None:
        """Test getting a non-existent run."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)

        run = await manager.get_run("nonexistent")
        assert run is None

    async def test_create_run(self, runs_dir: Path, mock_scraper: Any) -> None:
        """Test creating a new run."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)

        run = await manager.create_run("new_run", mock_scraper)

        assert run.run_id == "new_run"
        assert run.status == "loaded"
        assert run.driver is not None
        assert run.db_path.exists()
        assert "new_run" in manager.runs

        # Cleanup
        await run.driver.close()

    async def test_create_run_duplicate_raises(
        self, runs_dir: Path, mock_scraper: Any
    ) -> None:
        """Test that creating duplicate run raises ValueError."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)

        run = await manager.create_run("duplicate", mock_scraper)

        with pytest.raises(ValueError, match="already exists"):
            await manager.create_run("duplicate", mock_scraper)

        # Cleanup
        assert run.driver is not None
        await run.driver.close()

    async def test_load_run(self, runs_dir: Path, mock_scraper: Any) -> None:
        """Test loading an existing unloaded run."""
        from kent.driver.dev_driver.database import (
            init_database,
        )
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        # Create a database file first
        db_path = runs_dir / "existing.db"
        engine, session_factory = await init_database(db_path)

        # Insert run_metadata row since LocalDevDriver expects it on resume
        async with session_factory() as session:
            await session.execute(
                sa.text("""
                INSERT INTO run_metadata (id, scraper_name, status, base_delay, jitter, num_workers, max_backoff_time)
                VALUES (1, 'MockScraper', 'completed', 1.0, 0.5, 1, 60.0)
                """)
            )
            await session.commit()
        await engine.dispose()

        manager = RunManager(runs_dir)
        await manager.scan_runs()

        assert manager.runs["existing"].status == "unloaded"

        run = await manager.load_run("existing", mock_scraper)

        assert run.status == "loaded"
        assert run.driver is not None

        # Cleanup
        await run.driver.close()

    async def test_load_run_not_found(
        self, runs_dir: Path, mock_scraper: Any
    ) -> None:
        """Test loading non-existent run raises ValueError."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)

        with pytest.raises(ValueError, match="not found"):
            await manager.load_run("nonexistent", mock_scraper)

    async def test_unload_run(self, runs_dir: Path, mock_scraper: Any) -> None:
        """Test unloading a loaded run."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)
        run = await manager.create_run("to_unload", mock_scraper)

        assert run.status == "loaded"
        assert run.driver is not None

        await manager.unload_run("to_unload")

        assert manager.runs["to_unload"].status == "unloaded"
        assert manager.runs["to_unload"].driver is None

    async def test_delete_run(self, runs_dir: Path, mock_scraper: Any) -> None:
        """Test deleting a run and its database."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)
        run = await manager.create_run("to_delete", mock_scraper)
        db_path = run.db_path

        assert db_path.exists()

        # Unload first
        await manager.unload_run("to_delete")

        # Delete
        await manager.delete_run("to_delete")

        assert "to_delete" not in manager.runs
        assert not db_path.exists()

    async def test_delete_run_running_raises(
        self, runs_dir: Path, mock_scraper: Any
    ) -> None:
        """Test that deleting a running run raises ValueError."""
        from unittest.mock import MagicMock

        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)
        run = await manager.create_run("running", mock_scraper)

        # Simulate a running task
        run.task = MagicMock()
        run.task.done.return_value = False
        manager.runs["running"] = run

        with pytest.raises(ValueError, match="still running"):
            await manager.delete_run("running")

        # Cleanup
        assert run.driver is not None
        await run.driver.close()

    async def test_run_info_to_dict(self, runs_dir: Path) -> None:
        """Test RunInfo serialization."""
        from datetime import datetime, timezone

        from kent.driver.dev_driver.web.app import (
            RunInfo,
        )

        run_info = RunInfo(
            run_id="test",
            db_path=runs_dir / "test.db",
            status="running",
            created_at=datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc),
            started_at=datetime(2024, 1, 1, 12, 0, 1, tzinfo=timezone.utc),
        )

        d = run_info.to_dict()

        assert d["run_id"] == "test"
        assert d["status"] == "running"
        assert "2024-01-01" in d["created_at"]
        assert d["started_at"] is not None

    async def test_shutdown_all_empty(self, runs_dir: Path) -> None:
        """Test shutdown_all with no runs."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)

        # Should not raise
        await manager.shutdown_all()

    async def test_shutdown_all_unloads_all(
        self, runs_dir: Path, mock_scraper: Any
    ) -> None:
        """Test shutdown_all closes all driver connections."""
        from kent.driver.dev_driver.web.app import (
            RunManager,
        )

        manager = RunManager(runs_dir)
        run1 = await manager.create_run("run1", mock_scraper)
        run2 = await manager.create_run("run2", mock_scraper)

        assert run1.driver is not None
        assert run2.driver is not None

        await manager.shutdown_all()

        assert manager.runs["run1"].driver is None
        assert manager.runs["run2"].driver is None
        assert manager.runs["run1"].status == "unloaded"
        assert manager.runs["run2"].status == "unloaded"


class TestFastAPIApp:
    """Tests for FastAPI application setup."""

    def test_create_app(self, tmp_path: Path) -> None:
        """Test creating FastAPI app with custom runs directory."""
        from kent.driver.dev_driver.web.app import (
            create_app,
        )

        runs_dir = tmp_path / "custom_runs"
        app = create_app(runs_dir)

        assert app.state.runs_dir == runs_dir
        assert app.title == "LocalDevDriver Web Interface"

    def test_create_app_default_dir(self) -> None:
        """Test creating FastAPI app with default runs directory."""
        from kent.driver.dev_driver.web.app import (
            create_app,
        )

        app = create_app()

        assert app.state.runs_dir == Path("runs")

    def test_get_run_manager_not_initialized(self) -> None:
        """Test get_run_manager raises when not initialized."""
        from kent.driver.dev_driver.web import (
            app as app_module,
        )
        from kent.driver.dev_driver.web.app import (
            get_run_manager,
        )

        # Ensure no manager is set
        app_module._run_manager = None

        with pytest.raises(RuntimeError, match="not initialized"):
            get_run_manager()

    async def test_lifespan_initializes_manager(self, tmp_path: Path) -> None:
        """Test that lifespan initializes and cleans up run manager."""
        from kent.driver.dev_driver.web import (
            app as app_module,
        )
        from kent.driver.dev_driver.web.app import (
            create_app,
            get_run_manager,
            lifespan,
        )

        runs_dir = tmp_path / "lifespan_runs"
        app = create_app(runs_dir)

        # Before lifespan, manager should not be available
        app_module._run_manager = None

        async with lifespan(app):
            # During lifespan, manager should be available
            manager = get_run_manager()
            assert manager is not None
            assert manager.runs_dir == runs_dir
            assert runs_dir.exists()  # Should create directory

        # After lifespan, manager should be cleared
        assert app_module._run_manager is None


class TestRunsAPI:
    """Tests for /api/runs REST endpoints."""

    @pytest.fixture
    def runs_dir(self, tmp_path: Path) -> Path:
        """Create a temporary runs directory."""
        runs = tmp_path / "runs"
        runs.mkdir()
        return runs

    @pytest.fixture
    def test_app(self, runs_dir: Path):
        """Create test FastAPI app with custom runs directory."""
        from kent.driver.dev_driver.web.app import (
            create_app,
        )

        return create_app(runs_dir)

    @pytest.fixture
    def client(self, test_app):
        """Create TestClient for the app."""
        from fastapi.testclient import TestClient

        return TestClient(test_app)

    def test_list_runs_empty(self, client) -> None:
        """Test listing runs when empty."""
        with client:
            response = client.get("/api/runs")

        assert response.status_code == 200
        data = response.json()
        assert data["runs"] == []
        assert data["total"] == 0

    def test_list_runs_with_databases(self, runs_dir: Path, client) -> None:
        """Test listing runs with existing databases."""
        # Create some db files before starting the app
        (runs_dir / "test1.db").touch()
        (runs_dir / "test2.db").touch()

        with client:
            response = client.get("/api/runs")

        assert response.status_code == 200
        data = response.json()
        assert data["total"] == 2
        run_ids = {r["run_id"] for r in data["runs"]}
        assert run_ids == {"test1", "test2"}

    def test_get_run_not_found(self, client) -> None:
        """Test getting a non-existent run."""
        with client:
            response = client.get("/api/runs/nonexistent")

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()

    def test_get_run_found(self, runs_dir: Path, client) -> None:
        """Test getting an existing run."""
        (runs_dir / "existing.db").touch()

        with client:
            response = client.get("/api/runs/existing")

        assert response.status_code == 200
        data = response.json()
        assert data["run_id"] == "existing"
        assert data["status"] == "unloaded"

    def test_delete_run_not_found(self, client) -> None:
        """Test deleting a non-existent run."""
        with client:
            response = client.delete("/api/runs/nonexistent")

        assert response.status_code == 404

    def test_delete_run_success(self, runs_dir: Path, client) -> None:
        """Test successfully deleting a run."""
        db_path = runs_dir / "to_delete.db"
        db_path.touch()
        assert db_path.exists()

        with client:
            # First verify it exists
            response = client.get("/api/runs/to_delete")
            assert response.status_code == 200

            # Delete it
            response = client.delete("/api/runs/to_delete")
            assert response.status_code == 204

            # Verify it's gone
            response = client.get("/api/runs/to_delete")
            assert response.status_code == 404

        assert not db_path.exists()

    def test_scan_runs(self, runs_dir: Path, client) -> None:
        """Test scanning for new runs."""
        with client:
            # Start with no runs
            response = client.get("/api/runs")
            assert response.json()["total"] == 0

            # Create a new db file
            (runs_dir / "new_run.db").touch()

            # Scan
            response = client.post("/api/runs/scan")
            assert response.status_code == 200
            data = response.json()
            assert data["total"] == 1
            assert data["runs"][0]["run_id"] == "new_run"

    def test_start_run_not_found(self, client) -> None:
        """Test starting a non-existent run."""
        with client:
            response = client.post("/api/runs/nonexistent/start")

        assert response.status_code == 404

    def test_start_run_not_loaded(self, runs_dir: Path, client) -> None:
        """Test starting an unloaded run fails."""
        (runs_dir / "unloaded.db").touch()

        with client:
            response = client.post("/api/runs/unloaded/start")

        # Should fail because run is not loaded
        assert response.status_code == 400
        assert "not loaded" in response.json()["detail"].lower()

    def test_stop_run_not_found(self, client) -> None:
        """Test stopping a non-existent run."""
        with client:
            response = client.post("/api/runs/nonexistent/stop")

        assert response.status_code == 404

    def test_stop_run_not_running(self, runs_dir: Path, client) -> None:
        """Test stopping a non-running run fails."""
        (runs_dir / "not_running.db").touch()

        with client:
            response = client.post("/api/runs/not_running/stop")

        # Should fail because run is not running
        assert response.status_code == 400
        assert "not running" in response.json()["detail"].lower()

    def test_unload_run_not_found(self, client) -> None:
        """Test unloading a non-existent run."""
        with client:
            response = client.post("/api/runs/nonexistent/unload")

        assert response.status_code == 404

    def test_create_run_scraper_not_found(self, client) -> None:
        """Test creating a run with unknown scraper returns 404."""
        with client:
            response = client.post(
                "/api/runs",
                json={
                    "run_id": "new_run",
                    "scraper_path": "test.module:TestScraper",
                },
            )

        assert response.status_code == 404
        assert "not found" in response.json()["detail"].lower()


class TestWebSocketManager:
    """Tests for WebSocket manager."""

    async def test_connect_and_disconnect(self) -> None:
        """Test connecting and disconnecting WebSocket."""
        from unittest.mock import AsyncMock, MagicMock

        from kent.driver.dev_driver.web.websocket import (
            WebSocketManager,
        )

        manager = WebSocketManager()

        # Create mock WebSocket
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_text = AsyncMock()

        # Connect
        await manager.connect(ws, "test_run")

        assert manager.get_connection_count("test_run") == 1
        assert manager.get_total_connections() == 1
        ws.accept.assert_called_once()

        # Disconnect
        await manager.disconnect(ws, "test_run")

        assert manager.get_connection_count("test_run") == 0
        assert manager.get_total_connections() == 0

    async def test_broadcast(self) -> None:
        """Test broadcasting events to subscribers."""
        from datetime import datetime, timezone
        from unittest.mock import AsyncMock, MagicMock

        from kent.driver.dev_driver.dev_driver import (
            ProgressEvent,
        )
        from kent.driver.dev_driver.web.websocket import (
            WebSocketManager,
        )

        manager = WebSocketManager()

        # Create mock WebSockets
        ws1 = MagicMock()
        ws1.accept = AsyncMock()
        ws1.send_text = AsyncMock()

        ws2 = MagicMock()
        ws2.accept = AsyncMock()
        ws2.send_text = AsyncMock()

        # Connect both
        await manager.connect(ws1, "test_run")
        await manager.connect(ws2, "test_run")

        # Create event
        event = ProgressEvent(
            event_type="request_completed",
            timestamp=datetime.now(timezone.utc),
            data={"request_id": 1, "url": "https://example.com"},
        )

        # Broadcast
        await manager.broadcast("test_run", event)

        # Both should receive
        ws1.send_text.assert_called_once()
        ws2.send_text.assert_called_once()

    async def test_subscription_filtering(self) -> None:
        """Test that events are filtered by subscription."""
        from datetime import datetime, timezone
        from unittest.mock import AsyncMock, MagicMock

        from kent.driver.dev_driver.dev_driver import (
            ProgressEvent,
        )
        from kent.driver.dev_driver.web.websocket import (
            ProgressEventType,
            WebSocketManager,
        )

        manager = WebSocketManager()

        # Create mock WebSocket
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_text = AsyncMock()

        # Connect with limited subscription
        await manager.connect(
            ws, "test_run", event_types={ProgressEventType.REQUEST_COMPLETED}
        )

        # Event that matches subscription
        event_match = ProgressEvent(
            event_type="request_completed",
            timestamp=datetime.now(timezone.utc),
            data={"request_id": 1},
        )

        await manager.broadcast("test_run", event_match)
        assert ws.send_text.call_count == 1

        # Event that doesn't match subscription
        event_no_match = ProgressEvent(
            event_type="request_started",
            timestamp=datetime.now(timezone.utc),
            data={"request_id": 2},
        )

        await manager.broadcast("test_run", event_no_match)
        # Should still be 1, not called for this event
        assert ws.send_text.call_count == 1

    async def test_update_subscription(self) -> None:
        """Test updating subscription."""
        from unittest.mock import AsyncMock, MagicMock

        from kent.driver.dev_driver.web.websocket import (
            ProgressEventType,
            WebSocketManager,
        )

        manager = WebSocketManager()

        ws = MagicMock()
        ws.accept = AsyncMock()

        # Connect with all events (default)
        await manager.connect(ws, "test_run")

        # Check all events are subscribed
        assert len(manager._subscriptions[ws]) == len(ProgressEventType)

        # Update to limited subscription
        await manager.update_subscription(
            ws,
            {
                ProgressEventType.REQUEST_COMPLETED,
                ProgressEventType.ERROR_STORED,
            },
        )

        assert len(manager._subscriptions[ws]) == 2

    def test_progress_event_types(self) -> None:
        """Test ProgressEventType enum values."""
        from kent.driver.dev_driver.web.websocket import (
            ProgressEventType,
        )

        assert ProgressEventType.REQUEST_STARTED.value == "request_started"
        assert ProgressEventType.REQUEST_COMPLETED.value == "request_completed"
        assert ProgressEventType.ERROR_STORED.value == "error_stored"
        assert ProgressEventType.RUN_COMPLETED.value == "run_completed"

    async def test_create_progress_callback(self) -> None:
        """Test creating a progress callback for a driver."""
        from datetime import datetime, timezone
        from unittest.mock import AsyncMock, MagicMock

        from kent.driver.dev_driver.dev_driver import (
            ProgressEvent,
        )
        from kent.driver.dev_driver.web.websocket import (
            create_progress_callback,
            ws_manager,
        )

        # Create mock websocket and connect
        ws = MagicMock()
        ws.accept = AsyncMock()
        ws.send_text = AsyncMock()

        await ws_manager.connect(ws, "callback_test")

        # Create callback
        callback = create_progress_callback("callback_test")

        # Call callback with event
        event = ProgressEvent(
            event_type="request_completed",
            timestamp=datetime.now(timezone.utc),
            data={"request_id": 1},
        )

        await callback(event)

        # Should have broadcasted to ws
        ws.send_text.assert_called_once()

        # Cleanup
        await ws_manager.disconnect(ws, "callback_test")
