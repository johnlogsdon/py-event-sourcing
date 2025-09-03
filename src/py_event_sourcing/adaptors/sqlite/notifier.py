from typing import Dict, List
import aiosqlite
import json
import logging
from datetime import datetime
import asyncio
from collections import defaultdict

from py_event_sourcing.models import StoredEvent
from py_event_sourcing.protocols import Notifier


async def _fetch_new_events(conn: aiosqlite.Connection, last_id: int) -> List[StoredEvent]:
    """
    A simple, stateless function to query for all new events after a given ID.
    """
    query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version FROM events WHERE id > ? ORDER BY id"
    events = []
    try:
        async with conn.execute(query, (last_id,)) as cursor:
            async for row in cursor:
                (
                    _id,
                    stream_id,
                    idempotency_key,
                    event_type,
                    ts,
                    meta,
                    data,
                    version,
                ) = row
                try:
                    events.append(
                        StoredEvent(
                            id=idempotency_key or str(_id),
                            stream_id=stream_id,
                            sequence_id=_id,
                            type=event_type,
                            timestamp=datetime.fromisoformat(ts),
                            metadata=json.loads(meta) if meta else None,
                            data=data,
                            version=version,
                        )
                    )
                except Exception as e:
                    logging.warning(f"Notifier skipping malformed event row with id {_id}: {e}")
    except aiosqlite.OperationalError as e:
        logging.error(f"Database error during polling: {e}")
    return events


class SQLiteNotifier(Notifier):
    """A notifier that polls the database for new events."""

    def __init__(self, conn: aiosqlite.Connection, polling_interval: float = 0.2):
        self._conn = conn
        self._polling_interval = polling_interval
        self._watchers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._last_id = 0
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    @property
    def conn(self) -> aiosqlite.Connection:
        return self._conn

    async def start(self):
        """Starts the background polling task."""
        async with self._lock:
            if self._task is None:
                cursor = await self._conn.execute("SELECT MAX(id) FROM events")
                row = await cursor.fetchone()
                self._last_id = row[0] if row and row[0] is not None else 0
                self._task = asyncio.create_task(self._poll_for_changes())
                logging.info(f"Notifier started, last_id={self._last_id}")

    async def stop(self):
        """Stops the background polling task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        logging.info("Notifier stopped")

    async def _poll_for_changes(self):
        """The single background task that polls the events table."""
        while True:
            try:
                # The entire check-and-distribute operation must be inside the lock
                # to prevent race conditions that lead to deadlocks.
                async with self._lock:
                    new_events = await _fetch_new_events(self._conn, self._last_id)
                    
                    if not new_events:
                        continue
                    
                    for event in new_events:
                        # Notify watchers for the specific stream
                        if event.stream_id in self._watchers:
                            for queue in self._watchers[event.stream_id]:
                                await queue.put(event)
                        
                        # Also notify @all watchers
                        if "@all" in self._watchers:
                            for queue in self._watchers["@all"]:
                                await queue.put(event)
                    
                    # Update the last seen ID only after successfully processing the batch
                    self._last_id = new_events[-1].sequence_id

            except Exception as e:
                logging.error(f"Notifier poll loop error: {e}")
            
            await asyncio.sleep(self._polling_interval)

    async def subscribe(self, stream_id: str) -> asyncio.Queue:
        """Allows a watcher to subscribe to a stream_id."""
        async with self._lock:
            queue = asyncio.Queue()
            self._watchers[stream_id].append(queue)
            return queue

    async def unsubscribe(self, stream_id: str, queue: asyncio.Queue):
        """Removes a watcher's queue."""
        async with self._lock:
            if stream_id in self._watchers and queue in self._watchers[stream_id]:
                self._watchers[stream_id].remove(queue)
                if not self._watchers[stream_id]:
                    del self._watchers[stream_id]

