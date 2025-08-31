"""
This module provides the SQLite-specific implementation of the `StorageHandle`
protocol. It is responsible for all direct database interactions, such as
writing events, reading events, and managing snapshots within a SQLite database.
The use of `SAVEPOINT` is a key feature for ensuring atomicity on a shared connection.
"""
from typing import AsyncIterable, AsyncIterator, List, Set, Dict
from contextlib import asynccontextmanager
import aiosqlite
import json
import logging
import pydantic_core
from datetime import datetime
import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime

from ..models import Event, Snapshot
from ..protocols import StorageHandle

class SQLiteHandle(StorageHandle):
    """
    A concrete implementation of the `StorageHandle` protocol for SQLite.

    This class encapsulates all SQL queries and logic required to persist and
    retrieve event sourcing data from a SQLite database.
    """
    def __init__(self, conn: aiosqlite.Connection, stream_id: str, initial_version: int):
        self.conn = conn
        self.stream_id = stream_id
        self.version = initial_version

    async def sync(self, new_events: List[Event], expected_version: int):
        """
        Persist new events to the SQLite database in a transaction,
        atomically checking for the expected version.
        """
        try:
            # Use a savepoint to manage the transaction within the shared connection.
            # This provides transaction-like atomicity without requiring an exclusive
            # lock on the entire database, which is crucial because the connection
            # is shared across multiple stream instances.
            await self.conn.execute("SAVEPOINT event_append")

            # 1. Get current version inside the transaction
            cursor = await self.conn.execute(
                "SELECT MAX(version) FROM events WHERE stream_id = ?", (self.stream_id,)
            )
            row = await cursor.fetchone()
            await cursor.close()
            current_version = row[0] if row and row[0] is not None else 0

            # 2. Check for concurrency conflict
            if expected_version != -1 and current_version != expected_version:
                raise ValueError(
                    f"Concurrency conflict: expected version {expected_version}, but stream is at {current_version}"
                )

            # 3. Insert new events if any
            if new_events:
                # Assign versions to new events
                for i, event in enumerate(new_events):
                    event.version = current_version + i + 1

                params = [
                    (event.id, event.type, event.timestamp.isoformat(), json.dumps(event.metadata) if event.metadata else None, event.data, event.version)
                    for event in new_events
                ]
                await self.conn.executemany(
                    "INSERT INTO events (stream_id, idempotency_key, event_type, timestamp, metadata, data, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [(self.stream_id, *p) for p in params],
                )

            await self.conn.execute("RELEASE SAVEPOINT event_append")
            await self.conn.commit()
        except Exception as e:
            await self.conn.execute("ROLLBACK TO SAVEPOINT event_append")
            logging.error(f"Failed to sync events to SQLite: {e}")
            await self.conn.commit()  # Ensure transaction is closed even on error
            raise

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """Given a list of event IDs, query the DB and return the set that already exist."""
        if not event_ids:
            return set()
        placeholders = ",".join("?" for _ in event_ids)
        query = f"SELECT idempotency_key FROM events WHERE stream_id = ? AND idempotency_key IN ({placeholders})"
        params = [self.stream_id] + event_ids
        existing_ids = set()
        async with self.conn.execute(query, params) as cursor:
            async for row in cursor:
                existing_ids.add(row[0])
        return existing_ids

    async def get_events(self, start_version: int = 0) -> AsyncIterable[Event]:
        """An async generator to stream events from the database for a given stream_id, starting from a specific version."""
        async with self.conn.execute(
            "SELECT idempotency_key, event_type, timestamp, metadata, data, version FROM events WHERE stream_id = ? AND version > ? ORDER BY version",
            (self.stream_id, start_version,),
        ) as cursor:
            async for row in cursor:
                idempotency_key, event_type, timestamp_str, metadata_json, data_blob, version = row
                try:
                    yield Event(
                        id=idempotency_key,
                        type=event_type,
                        timestamp=datetime.fromisoformat(timestamp_str),
                        metadata=json.loads(metadata_json) if metadata_json else None,
                        data=data_blob,
                        version=version,
                    )
                except (json.JSONDecodeError, pydantic_core.ValidationError, KeyError, ValueError) as e:
                    logging.warning(f"Skipping invalid event row during replay for stream {self.stream_id}: {e}")

    async def get_last_timestamp(self) -> datetime | None:
        """Returns the timestamp of the last event in the stream, or None if the stream is empty."""
        async with self.conn.execute(
            "SELECT timestamp FROM events WHERE stream_id = ? ORDER BY id DESC LIMIT 1",
            (self.stream_id,),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                return datetime.fromisoformat(row[0])
            return None

    async def save_snapshot(self, snapshot: Snapshot):
        """Saves a snapshot to the database, overwriting any existing snapshot for the same stream_id and projection_name."""
        await self.conn.execute(
            "INSERT OR REPLACE INTO snapshots (stream_id, projection_name, version, state, timestamp) VALUES (?, ?, ?, ?, ?)",
            (snapshot.stream_id, snapshot.projection_name, snapshot.version, snapshot.state, snapshot.timestamp.isoformat()),
        )
        await self.conn.commit()

    async def load_latest_snapshot(self, stream_id: str, projection_name: str = "default") -> Snapshot | None:
        """Loads the latest snapshot for a given stream_id and projection_name, or None if no snapshot exists."""
        async with self.conn.execute(
            "SELECT stream_id, projection_name, version, state, timestamp FROM snapshots WHERE stream_id = ? AND projection_name = ? ORDER BY version DESC LIMIT 1",
            (stream_id, projection_name),
        ) as cursor:
            row = await cursor.fetchone()
            if row:
                stream_id, proj_name, version, state, timestamp_str = row
                return Snapshot(
                    stream_id=stream_id,
                    projection_name=proj_name,
                    version=version,
                    state=state,
                    timestamp=datetime.fromisoformat(timestamp_str),
                )
            return None

    async def close(self):
        # The connection is now shared and managed by the factory, so the handle should not close it.
        pass


class SQLitePollingNotifier:
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

    async def start(self):
        """Ensures schema exists and starts the polling task."""
        if self._task:
            return
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
        await self._conn.commit()
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
                            event = Event(
                                id=idempotency_key,
                                type=event_type,
                                timestamp=datetime.fromisoformat(ts),
                                metadata=json.loads(meta) if meta else None,
                                data=data,
                                version=version,
                            )
                            if stream_id in self._watchers:
                                for queue in self._watchers[stream_id]:
                                    await queue.put(event)
                            self._last_id = _id
                        except Exception as e:
                            logging.warning(
                                f"Notifier skipping malformed event row with id {_id}: {e}"
                            )
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


@asynccontextmanager
async def sqlite_open_adapter(conn: aiosqlite.Connection, db_path: str, stream_id: str) -> AsyncIterator[StorageHandle]:
    """
    Adapter that uses a shared database connection.
    Schema initialization is now handled by the Notifier to ensure it happens
    only once per database, and is safe for production use.
    """
    # Get the initial version count for the stream. This is done here to ensure
    # the handle starts with the correct version number for this specific stream.
    async with conn.execute(
        "SELECT MAX(version) FROM events WHERE stream_id = ?",
        (stream_id,),
    ) as cursor:
        row = await cursor.fetchone()
        initial_version = row[0] if row and row[0] is not None else 0

    handle = SQLiteHandle(conn, stream_id, initial_version)
    try:
        yield handle
    finally:
        await handle.close()
