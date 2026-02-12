"""WebSocket support for real-time progress events.

This module provides WebSocket functionality for:
- Real-time progress updates from running scrapers
- Subscription management with selective event filtering
- Connection lifecycle handling
"""

from __future__ import annotations

import asyncio
import json
import logging
from enum import Enum
from typing import TYPE_CHECKING, Any

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

if TYPE_CHECKING:
    from kent.driver.dev_driver.dev_driver import (
        ProgressEvent,
    )

logger = logging.getLogger(__name__)

router = APIRouter()


class ProgressEventType(str, Enum):
    """Types of progress events that can be subscribed to."""

    REQUEST_STARTED = "request_started"
    REQUEST_COMPLETED = "request_completed"
    REQUEST_FAILED = "request_failed"
    RESULT_STORED = "result_stored"
    ERROR_STORED = "error_stored"
    RUN_STARTED = "run_started"
    RUN_STOPPED = "run_stopped"
    RUN_COMPLETED = "run_completed"
    STATS_UPDATED = "stats_updated"


class WebSocketManager:
    """Manager for WebSocket connections and event broadcasting.

    Handles:
    - Connection lifecycle (connect, disconnect)
    - Event subscriptions per connection
    - Broadcasting events to appropriate subscribers
    """

    def __init__(self) -> None:
        """Initialize the WebSocket manager."""
        # Map of run_id -> set of connections
        self._connections: dict[str, set[WebSocket]] = {}
        # Map of websocket -> set of event types subscribed to
        self._subscriptions: dict[WebSocket, set[ProgressEventType]] = {}
        self._lock = asyncio.Lock()

    async def connect(
        self,
        websocket: WebSocket,
        run_id: str,
        event_types: set[ProgressEventType] | None = None,
    ) -> None:
        """Accept a WebSocket connection and register it.

        Args:
            websocket: The WebSocket connection.
            run_id: The run to subscribe to.
            event_types: Optional set of event types to subscribe to.
                If None, subscribes to all events.
        """
        await websocket.accept()

        async with self._lock:
            if run_id not in self._connections:
                self._connections[run_id] = set()
            self._connections[run_id].add(websocket)

            # Subscribe to all events by default
            if event_types is None:
                event_types = set(ProgressEventType)
            self._subscriptions[websocket] = event_types

        logger.info(
            f"WebSocket connected for run '{run_id}' with {len(event_types)} event types"
        )

    async def disconnect(self, websocket: WebSocket, run_id: str) -> None:
        """Remove a WebSocket connection.

        Args:
            websocket: The WebSocket connection to remove.
            run_id: The run it was subscribed to.
        """
        async with self._lock:
            if run_id in self._connections:
                self._connections[run_id].discard(websocket)
                if not self._connections[run_id]:
                    del self._connections[run_id]
            self._subscriptions.pop(websocket, None)

        logger.info(f"WebSocket disconnected from run '{run_id}'")

    async def update_subscription(
        self,
        websocket: WebSocket,
        event_types: set[ProgressEventType],
    ) -> None:
        """Update the event types a connection is subscribed to.

        Args:
            websocket: The WebSocket connection.
            event_types: New set of event types to subscribe to.
        """
        async with self._lock:
            if websocket in self._subscriptions:
                self._subscriptions[websocket] = event_types

    async def broadcast(self, run_id: str, event: ProgressEvent) -> None:
        """Broadcast a progress event to all subscribed connections.

        Args:
            run_id: The run that generated the event.
            event: The progress event to broadcast.
        """
        async with self._lock:
            connections = self._connections.get(run_id, set()).copy()

        if not connections:
            return

        # Try to map event type to ProgressEventType
        try:
            event_type = ProgressEventType(event.event_type)
        except ValueError:
            event_type = None

        message = event.to_json()

        for websocket in connections:
            # Check if this connection wants this event type
            subscribed_types = self._subscriptions.get(websocket, set())
            if event_type is not None and event_type not in subscribed_types:
                continue

            try:
                await websocket.send_text(message)
            except Exception as e:
                logger.warning(f"Failed to send to WebSocket: {e}")
                # Don't remove here - let the disconnect handler clean up

    async def broadcast_to_all(self, event: ProgressEvent) -> None:
        """Broadcast an event to all connected clients.

        Args:
            event: The progress event to broadcast.
        """
        async with self._lock:
            all_runs = list(self._connections.keys())

        for run_id in all_runs:
            await self.broadcast(run_id, event)

    def get_connection_count(self, run_id: str) -> int:
        """Get the number of connections for a run.

        Args:
            run_id: The run identifier.

        Returns:
            Number of active connections.
        """
        return len(self._connections.get(run_id, set()))

    def get_total_connections(self) -> int:
        """Get the total number of WebSocket connections.

        Returns:
            Total connection count.
        """
        return sum(len(conns) for conns in self._connections.values())


# Global WebSocket manager
ws_manager = WebSocketManager()


def get_ws_manager() -> WebSocketManager:
    """Get the global WebSocket manager.

    Returns:
        The WebSocketManager instance.
    """
    return ws_manager


def create_progress_callback(run_id: str) -> Any:
    """Create a progress callback for a driver that broadcasts to WebSocket.

    Args:
        run_id: The run identifier.

    Returns:
        Async callback function for driver's on_progress.
    """

    async def callback(event: ProgressEvent) -> None:
        await ws_manager.broadcast(run_id, event)

    return callback


@router.websocket("/ws/runs/{run_id}")
async def websocket_endpoint(websocket: WebSocket, run_id: str) -> None:
    """WebSocket endpoint for real-time progress events.

    Connect to receive progress events for a specific run.

    The client can send JSON messages to control the subscription:
    - {"action": "subscribe", "events": ["request_started", "request_completed"]}
    - {"action": "unsubscribe", "events": ["stats_updated"]}
    - {"action": "subscribe_all"}

    Args:
        websocket: The WebSocket connection.
        run_id: The run to receive events from.
    """
    from kent.driver.dev_driver.web.app import (
        get_run_manager,
    )

    # Verify run exists
    try:
        manager = get_run_manager()
        run_info = await manager.get_run(run_id)
        if run_info is None:
            await websocket.close(
                code=4004, reason=f"Run '{run_id}' not found"
            )
            return
    except RuntimeError:
        await websocket.close(code=4000, reason="Server not initialized")
        return

    await ws_manager.connect(websocket, run_id)

    try:
        while True:
            # Receive messages for subscription control
            data = await websocket.receive_text()

            try:
                message = json.loads(data)
                action = message.get("action")

                if action == "subscribe":
                    events = message.get("events", [])
                    event_types = {
                        ProgressEventType(e)
                        for e in events
                        if e in ProgressEventType.__members__.values()
                    }
                    current = ws_manager._subscriptions.get(websocket, set())
                    await ws_manager.update_subscription(
                        websocket, current | event_types
                    )
                    await websocket.send_json(
                        {
                            "status": "subscribed",
                            "events": [e.value for e in event_types],
                        }
                    )

                elif action == "unsubscribe":
                    events = message.get("events", [])
                    event_types = {
                        ProgressEventType(e)
                        for e in events
                        if e in ProgressEventType.__members__.values()
                    }
                    current = ws_manager._subscriptions.get(websocket, set())
                    await ws_manager.update_subscription(
                        websocket, current - event_types
                    )
                    await websocket.send_json(
                        {
                            "status": "unsubscribed",
                            "events": [e.value for e in event_types],
                        }
                    )

                elif action == "subscribe_all":
                    await ws_manager.update_subscription(
                        websocket, set(ProgressEventType)
                    )
                    await websocket.send_json({"status": "subscribed_all"})

                elif action == "ping":
                    await websocket.send_json({"status": "pong"})

                else:
                    await websocket.send_json(
                        {"error": f"Unknown action: {action}"}
                    )

            except json.JSONDecodeError:
                await websocket.send_json({"error": "Invalid JSON"})
            except Exception as e:
                await websocket.send_json({"error": str(e)})

    except WebSocketDisconnect:
        pass
    finally:
        await ws_manager.disconnect(websocket, run_id)


@router.get("/ws/status")
async def websocket_status() -> dict[str, Any]:
    """Get WebSocket connection status.

    Returns:
        Connection statistics.
    """
    return {
        "total_connections": ws_manager.get_total_connections(),
        "runs_with_connections": len(ws_manager._connections),
    }
