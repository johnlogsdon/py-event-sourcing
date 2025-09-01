from typing import Dict, List
import aiosqlite
import json
import logging
from datetime import datetime
import asyncio
from collections import defaultdict

from event_sourcing_v2.models import StoredEvent
from event_sourcing_v2.protocols import Notifier


class SQLiteNotifier(Notifier):
    """
    A centralized watcher that polls the database once for all new events
    and dispatches them to the appropriate stream watchers.

    This is the SQLite-specific implementation of the Notifier protocol and is
    a core part of the library's efficiency at scale.
    """

    def __init__(self, conn: aiosqlite.Connection, polling_interval: float = 0.2):
        self._polling_interval = polling_interval
        self._conn = conn
        self._watchers: Dict[str, List[asyncio.Queue]] = defaultdict(list)
        self._last_id = 0
        self._task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

    async def _create_schema(self):
        # Schema management is centralized here. The first notifier created for a
        # given database file is responsible for ensuring the necessary tables
        # and indexes exist.
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL,
                idempotency_key TEXT,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                metadata TEXT,
                data BLOB NOT NULL,
                version INTEGER NOT NULL DEFAULT 0
            )
        """
        )
        # Create an index on stream_id and version for fast lookups and ordering.
        # This is critical for performance on `read()` and `write()` operations.
        await self._conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_event_stream_version ON events (stream_id, version)
            """
        )
        # Create an index on the idempotency key. This is crucial for performance
        # of the `find_existing_ids` query on very large tables.
        # The WHERE clause creates a partial index, only for rows where the key is not null.
        await self._conn.execute(
            """
            CREATE UNIQUE INDEX IF NOT EXISTS idx_event_idempotency
            ON events (stream_id, idempotency_key) WHERE idempotency_key IS NOT NULL
        """
        )
        # Also create the snapshots table here, so schema is managed centrally.
        await self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS snapshots (
                stream_id TEXT NOT NULL,
                projection_name TEXT NOT NULL,
                version INTEGER NOT NULL,
                state BLOB NOT NULL,
                timestamp TEXT NOT NULL,
                PRIMARY KEY (stream_id, projection_name)
            )
        """
        )

    async def start(self):
        """Ensures schema exists and starts the polling task."""
        if self._task:
            return
        await self._create_schema()
        # Get the last known event ID to start polling from.
        async with self._conn.execute("SELECT MAX(id) FROM events") as cursor:
            row = await cursor.fetchone()
            self._last_id = row[0] if row and row[0] is not None else 0
        self._task = asyncio.create_task(self._poll_for_changes())
        logging.info(f"Notifier started, polling from ID {self._last_id}")

    async def stop(self):
        """Stops the polling task."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        # The connection is managed by the factory, so we don't close it here.
        logging.info(f"Notifier stopped")

    async def _poll_for_changes(self):
        """The single background task that polls the events table."""
        while True:
            try:
                query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version FROM events WHERE id > ? ORDER BY id"
                last_id_in_batch = self._last_id # Store the starting point for this batch
                async with self._conn.execute(query, (self._last_id,)) as cursor:
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
                            event = StoredEvent(
                                id=idempotency_key or str(_id),
                                stream_id=stream_id,
                                sequence_id=_id,
                                type=event_type,
                                timestamp=datetime.fromisoformat(ts),
                                metadata=json.loads(meta) if meta else None,
                                data=data,
                                version=version,
                            )
                            if stream_id in self._watchers:
                                for queue in self._watchers[stream_id]:
                                    await queue.put(event)
                            last_id_in_batch = _id # Update temporary variable
                            
                            # Also notify @all watchers
                            if stream_id != "@all" and "@all" in self._watchers:
                                for queue in self._watchers["@all"]:
                                    await queue.put(event)
                        except Exception as e:
                            logging.warning(
                                f"Notifier skipping malformed event row with id {_id}: {e}"
                            )
                self._last_id = last_id_in_batch # Only update if entire batch processed
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
