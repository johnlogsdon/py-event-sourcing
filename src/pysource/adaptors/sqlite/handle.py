from typing import AsyncIterable, AsyncIterator, List, Set
from contextlib import asynccontextmanager
import aiosqlite
import json
import logging
import pydantic_core
from datetime import datetime
import asyncio
import uuid

from pysource.models import CandidateEvent, StoredEvent, Snapshot
from pysource.protocols import StorageHandle


class SQLiteStorageHandle(StorageHandle):
    """
    A handle that manages read and write operations for a specific stream
    using a dedicated write connection and a pool of read connections.
    """

    def __init__(
        self,
        stream_id: str,
        write_conn: aiosqlite.Connection,
        write_lock: asyncio.Lock,
        read_pool: asyncio.Queue,
    ):
        self.stream_id = stream_id
        self.write_conn = write_conn
        self.write_lock = write_lock
        self.read_pool = read_pool
        self.version = -1  # Uninitialized

    async def _async_init(self):
        """Initializes the stream's version from a read connection."""
        async with self._read_handle() as handle:
            self.version = handle.version

    @asynccontextmanager
    async def _read_handle(self) -> AsyncIterator[StorageHandle]:
        """Provides a handle with a connection from the read pool."""
        conn = await self.read_pool.get()
        try:
            handle = SQLiteHandle(conn, self.stream_id)
            await handle._async_init()
            yield handle
        finally:
            await self.read_pool.put(conn)

    async def sync(self, new_events: List[CandidateEvent], expected_version: int):
        """Persists new events using the dedicated write connection."""
        async with self.write_lock:
            async with sqlite_open_adapter(self.write_conn, self.stream_id) as handle:
                new_version = await handle.sync(self.stream_id, new_events, expected_version)
                self.version = new_version
                return new_version

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """Finds existing event IDs using the write connection, to ensure transaction consistency."""
        handle = SQLiteHandle(self.write_conn, self.stream_id)
        return await handle.find_existing_ids(event_ids)

    async def get_events(self, start_version: int = 0) -> AsyncIterable[StoredEvent]:
        """Gets events using a read connection."""
        conn = await self.read_pool.get()
        try:
            handle = SQLiteHandle(conn, self.stream_id)
            await handle._async_init()
            async for event in handle.get_events(start_version):
                yield event
        finally:
            await self.read_pool.put(conn)

    async def get_last_timestamp(self) -> datetime | None:
        """Gets the last timestamp using a read connection."""
        async with self._read_handle() as handle:
            return await handle.get_last_timestamp()

    async def save_snapshot(self, snapshot: Snapshot):
        """Saves a snapshot using the dedicated write connection."""
        async with self.write_lock:
            async with sqlite_open_adapter(self.write_conn, self.stream_id) as handle:
                await handle.save_snapshot(snapshot)
                self.version = snapshot.version

    async def load_latest_snapshot(
        self, stream_id: str, projection_name: str = "default"
    ) -> Snapshot | None:
        """Loads a snapshot using a read connection."""
        async with self._read_handle() as handle:
            return await handle.load_latest_snapshot(stream_id, projection_name)


class SQLiteHandle(StorageHandle):
    """
    A concrete implementation of the `StorageHandle` protocol for SQLite.

    This class encapsulates all SQL queries and logic required to persist and
    retrieve event sourcing data from a SQLite database.
    """
    def __init__(self, conn: aiosqlite.Connection, stream_id: str):
        self.conn = conn
        self.stream_id = stream_id
        self.version = -1 # Uninitialized

    async def _async_init(self):
        """Asynchronously initializes the stream's version."""
        if self.stream_id == "@all":
            self.version = 0 # @all stream does not have a version
            return
            
        async with self.conn.execute(
            "SELECT MAX(version) FROM events WHERE stream_id = ?", (self.stream_id,)
        ) as cursor:
            row = await cursor.fetchone()
            self.version = row[0] if row and row[0] is not None else 0

    async def sync(self, stream_id: str, new_events: List[CandidateEvent], expected_version: int):
        """
        Persist new events to the SQLite database in a transaction,
        atomically checking for the expected version.
        """
        # Filter out duplicates within the batch first
        if new_events:
            seen_ids = set()
            unique_new_events = []
            for e in new_events:
                if e.idempotency_key:
                    if e.idempotency_key not in seen_ids:
                        unique_new_events.append(e)
                        seen_ids.add(e.idempotency_key)
                else:
                    unique_new_events.append(e)
            new_events = unique_new_events
            
        # Idempotency check must be done inside the transaction.
        idempotency_keys_to_check = [e.idempotency_key for e in new_events if e.idempotency_key]
        if idempotency_keys_to_check:
            existing_ids = await self.find_existing_ids(idempotency_keys_to_check)
            new_events = [e for e in new_events if e.idempotency_key not in existing_ids]

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

            # 3. Filter out events that have already been persisted
            idempotency_keys_to_check = [e.idempotency_key for e in new_events if e.idempotency_key]
            if idempotency_keys_to_check:
                existing_ids = await self.find_existing_ids(idempotency_keys_to_check)
                new_events = [e for e in new_events if e.idempotency_key not in existing_ids]

            # 4. Insert new events if any
            if new_events:
                now = datetime.utcnow()
                events_to_insert = []
                for i, event in enumerate(new_events):
                    events_to_insert.append(
                        (
                            stream_id,
                            event.idempotency_key or str(uuid.uuid4()),
                            event.type,
                            now.isoformat(),
                            json.dumps(event.metadata) if event.metadata else None,
                            event.data,
                            current_version + i + 1,
                        )
                    )
                
                await self.conn.executemany(
                    "INSERT INTO events (stream_id, idempotency_key, event_type, timestamp, metadata, data, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                    events_to_insert,
                )

            await self.conn.execute("RELEASE SAVEPOINT event_append")
            return current_version + len(new_events) # Return the new version
        except Exception as e:
            await self.conn.execute("ROLLBACK TO SAVEPOINT event_append")
            logging.error(f"Failed to sync events to SQLite: {e}")
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

    async def get_events(self, start_version: int = 0) -> AsyncIterable[StoredEvent]:
        """An async generator to stream events from the database for a given stream_id, starting from a specific version."""
        if self.stream_id == "@all":
            query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version, id FROM events WHERE id > ? ORDER BY id"
            params = (start_version,)
        else:
            query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version, id FROM events WHERE stream_id = ? AND version > ? ORDER BY version"
            params = (self.stream_id, start_version,)

        async with self.conn.execute(query, params) as cursor:
            async for row in cursor:
                sequence_id, stream_id, idempotency_key, event_type, timestamp_str, metadata_json, data_blob, version, event_id = row
                try:
                    yield StoredEvent(
                        id=idempotency_key or str(event_id),
                        stream_id=stream_id,
                        sequence_id=sequence_id,
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

@asynccontextmanager
async def sqlite_open_adapter(conn: aiosqlite.Connection, stream_id: str) -> AsyncIterator[StorageHandle]:
    """
    Adapter that uses a shared database connection for write transactions.
    It begins a transaction on entry and commits on successful exit.
    """
    handle = SQLiteHandle(conn, stream_id)
    await handle._async_init()
    
    try:
        yield handle
        await conn.commit()
    except Exception:
        await conn.rollback()
        raise


@asynccontextmanager
async def sqlite_read_adapter(conn: aiosqlite.Connection, stream_id: str) -> AsyncIterator[StorageHandle]:
    """
    Adapter that provides a handle for read-only operations.
    No transaction is started or committed.
    """
    handle = SQLiteHandle(conn, stream_id)
    await handle._async_init()
    yield handle
