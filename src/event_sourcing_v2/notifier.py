import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime
from typing import Dict, List

import aiosqlite

from .models import Event


class Notifier:
    """
    A centralized watcher that polls the database once for all new events
    and dispatches them to the appropriate stream watchers. This avoids the
    "thundering herd" problem of many watchers polling the database individually.
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._watchers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._last_id = 0
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def start(self):
        """Connects to the DB and starts the polling task."""
        if self._task:
            return
        self._conn = await aiosqlite.connect(self._db_path)
        # Ensure the events table exists before we query it. This centralizes
        # schema management and prevents a race condition on first run.
        await self._conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                metadata TEXT NOT NULL,
                data BLOB NOT NULL
            )
        """)
        # Create an index on the idempotency key. This is crucial for performance
        # of the `find_existing_ids` query on very large tables.
        await self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_event_idempotency
            ON events (stream_id, json_extract(metadata, '$.id'))
        """)
        await self._conn.commit()
        # Get the last known event ID to start polling from.
        async with self._conn.execute("SELECT MAX(id) FROM events") as cursor:
            row = await cursor.fetchone()
            self._last_id = row[0] if row and row[0] is not None else 0
        self._task = asyncio.create_task(self._poll_for_changes())
        logging.info(f"Notifier started for {self._db_path}, polling from ID {self._last_id}")

    async def stop(self):
        """Stops the polling task and closes the connection."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._conn:
            await self._conn.close()
            self._conn = None
        logging.info(f"Notifier stopped for {self._db_path}")

    async def _poll_for_changes(self):
        """The single background task that polls the events table."""
        while True:
            try:
                query = "SELECT id, stream_id, event_type, timestamp, metadata, data FROM events WHERE id > ? ORDER BY id"
                async with self._conn.execute(query, (self._last_id,)) as cursor:
                    async for row in cursor:
                        _id, stream_id, event_type, ts, meta, data = row
                        try:
                            event = Event(type=event_type, timestamp=datetime.fromisoformat(ts), metadata=json.loads(meta), data=data)
                            if stream_id in self._watchers:
                                for queue in self._watchers[stream_id]:
                                    await queue.put(event)
                            self._last_id = _id
                        except Exception as e:
                            logging.warning(f"Notifier skipping malformed event row with id {_id}: {e}")
            except Exception as e:
                logging.error(f"Notifier poll loop error: {e}")
            await asyncio.sleep(0.2)

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