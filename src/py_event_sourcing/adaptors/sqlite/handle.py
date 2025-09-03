import asyncio
import json
import logging
import uuid
from contextlib import asynccontextmanager
from datetime import datetime
from typing import AsyncIterable, AsyncIterator, List, Set

import aiosqlite
import pydantic_core

from py_event_sourcing.models import CandidateEvent, EventFilter, Snapshot, StoredEvent
from py_event_sourcing.protocols import StorageHandle

from .filter_parser import parse_event_filter_to_sql

# -----------------------------------------------------------------------------
# Stateless Query Functions
# -----------------------------------------------------------------------------
# These functions are pure and stateless. They take a connection and all
# necessary parameters, execute a single piece of DB logic, and return a
# result. They are the building blocks for the stateful handle.
# -----------------------------------------------------------------------------


async def _get_stream_version(conn: aiosqlite.Connection, stream_id: str) -> int:
    """Gets the latest version number for a given stream."""
    if stream_id == "@all":
        return 0  # @all stream is not versioned in the same way

    async with conn.execute(
        "SELECT MAX(version) FROM events WHERE stream_id = ?", (stream_id,)
    ) as cursor:
        row = await cursor.fetchone()
        return row[0] if row and row[0] is not None else 0


async def _sync_events(
    conn: aiosqlite.Connection,
    stream_id: str,
    new_events: List[CandidateEvent],
    expected_version: int,
) -> int:
    """
    Persists new events to the database in a transaction, atomically checking
    for the expected version and idempotency.
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

    try:
        await conn.execute("SAVEPOINT event_append")

        # 1. Get current version and check for idempotency inside the transaction
        current_version = await _get_stream_version(conn, stream_id)
        idempotency_keys_to_check = [
            e.idempotency_key for e in new_events if e.idempotency_key
        ]
        if idempotency_keys_to_check:
            existing_ids = await _find_existing_ids(
                conn, stream_id, idempotency_keys_to_check
            )
            new_events = [
                e for e in new_events if e.idempotency_key not in existing_ids
            ]

        # 2. Check for concurrency conflict
        if expected_version != -1 and current_version != expected_version:
            raise ValueError(
                f"Concurrency conflict: expected version {expected_version}, but stream is at {current_version}"
            )

        # 3. Insert new events if any remain after idempotency check
        if new_events:
            now = datetime.utcnow()
            events_to_insert = [
                (
                    stream_id,
                    event.idempotency_key or str(uuid.uuid4()),
                    event.type,
                    now.isoformat(),
                    json.dumps(event.metadata) if event.metadata else None,
                    event.data,
                    current_version + i + 1,
                )
                for i, event in enumerate(new_events)
            ]
            await conn.executemany(
                "INSERT INTO events (stream_id, idempotency_key, event_type, timestamp, metadata, data, version) VALUES (?, ?, ?, ?, ?, ?, ?)",
                events_to_insert,
            )

        await conn.execute("RELEASE SAVEPOINT event_append")
        new_version = current_version + len(new_events)
        await conn.commit()
        return new_version

    except Exception as e:
        await conn.execute("ROLLBACK TO SAVEPOINT event_append")
        await conn.rollback()
        logging.error(f"Failed to sync events to SQLite: {e}")
        raise


async def _find_existing_ids(
    conn: aiosqlite.Connection, stream_id: str, event_ids: List[str]
) -> Set[str]:
    """Given a list of event IDs, query the DB and return the set that already exist."""
    if not event_ids:
        return set()
    placeholders = ",".join("?" for _ in event_ids)
    query = f"SELECT idempotency_key FROM events WHERE stream_id = ? AND idempotency_key IN ({placeholders})"
    params = [stream_id] + event_ids
    existing_ids = set()
    async with conn.execute(query, params) as cursor:
        async for row in cursor:
            existing_ids.add(row[0])
    return existing_ids


async def _get_events(
    conn: aiosqlite.Connection,
    stream_id: str,
    start_version: int = 0,
    event_filter: EventFilter | None = None,
) -> AsyncIterable[StoredEvent]:
    """An async generator to stream events from the database for a given stream_id."""

    params: list = []

    if stream_id == "@all":
        base_query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version, id FROM events"
        conditions = ["id > ?"]
        params.append(start_version)
    else:
        base_query = "SELECT id, stream_id, idempotency_key, event_type, timestamp, metadata, data, version, id FROM events"
        conditions = ["stream_id = ?", "version > ?"]
        params.extend([stream_id, start_version])

    if event_filter:
        filter_clause, filter_params = parse_event_filter_to_sql(event_filter)
        if filter_clause:
            # The generated clause includes "WHERE", so we remove it and join with "AND"
            conditions.append(filter_clause.replace("WHERE ", ""))
            params.extend(filter_params)

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    order_by = "ORDER BY id" if stream_id == "@all" else "ORDER BY version"

    final_query = f"{base_query} {where_clause} {order_by}"

    async with conn.execute(final_query, tuple(params)) as cursor:
        async for row in cursor:
            (
                sequence_id,
                stream_id_db,
                idempotency_key,
                event_type,
                timestamp_str,
                metadata_json,
                data_blob,
                version,
                event_id,
            ) = row
            try:
                yield StoredEvent(
                    id=idempotency_key or str(event_id),
                    stream_id=stream_id_db,
                    sequence_id=sequence_id,
                    type=event_type,
                    timestamp=datetime.fromisoformat(timestamp_str),
                    metadata=json.loads(metadata_json) if metadata_json else None,
                    data=data_blob,
                    version=version,
                )
            except (
                json.JSONDecodeError,
                pydantic_core.ValidationError,
                KeyError,
                ValueError,
            ) as e:
                logging.warning(
                    f"Skipping invalid event row during replay for stream {stream_id}: {e}"
                )


async def _get_last_timestamp(
    conn: aiosqlite.Connection, stream_id: str
) -> datetime | None:
    """Returns the timestamp of the last event in the stream."""
    async with conn.execute(
        "SELECT timestamp FROM events WHERE stream_id = ? ORDER BY id DESC LIMIT 1",
        (stream_id,),
    ) as cursor:
        row = await cursor.fetchone()
        return datetime.fromisoformat(row[0]) if row else None


async def _save_snapshot(conn: aiosqlite.Connection, snapshot: Snapshot):
    """Saves a snapshot to the database."""
    await conn.execute(
        "INSERT OR REPLACE INTO snapshots (stream_id, projection_name, version, state, timestamp) VALUES (?, ?, ?, ?, ?)",
        (
            snapshot.stream_id,
            snapshot.projection_name,
            snapshot.version,
            snapshot.state,
            snapshot.timestamp.isoformat(),
        ),
    )
    await conn.commit()


async def _load_latest_snapshot(
    conn: aiosqlite.Connection, stream_id: str, projection_name: str
) -> Snapshot | None:
    """Loads the latest snapshot for a given stream."""
    async with conn.execute(
        "SELECT stream_id, projection_name, version, state, timestamp FROM snapshots WHERE stream_id = ? AND projection_name = ? ORDER BY version DESC LIMIT 1",
        (stream_id, projection_name),
    ) as cursor:
        row = await cursor.fetchone()
        if row:
            stream_id_db, proj_name, version, state, timestamp_str = row
            return Snapshot(
                stream_id=stream_id_db,
                projection_name=proj_name,
                version=version,
                state=state,
                timestamp=datetime.fromisoformat(timestamp_str),
            )
        return None


# -----------------------------------------------------------------------------
# Stateful Storage Handle
# -----------------------------------------------------------------------------
# This class implements the StorageHandle protocol and manages the stateful
# aspects of a stream: its version, concurrency locks, and connection pools.
# It calls the stateless query functions above to do the actual DB work.
# -----------------------------------------------------------------------------


class SQLiteStorageHandle(StorageHandle):
    """
    A handle that manages read and write operations for a specific stream
    using a dedicated write connection and a pool of read connections.
    This object itself is stateless regarding the stream's version.
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

    async def get_version(self) -> int:
        """Retrieves the stream's version from a read connection."""
        async with self._get_read_conn() as conn:
            return await _get_stream_version(conn, self.stream_id)

    @asynccontextmanager
    async def _get_read_conn(self) -> AsyncIterator[aiosqlite.Connection]:
        """Provides a managed connection from the read pool."""
        conn = await self.read_pool.get()
        try:
            yield conn
        finally:
            await self.read_pool.put(conn)

    async def sync(
        self, new_events: List[CandidateEvent], expected_version: int
    ) -> int:
        """Persists new events using the dedicated write connection."""
        async with self.write_lock:
            return await _sync_events(
                self.write_conn, self.stream_id, new_events, expected_version
            )

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """Finds existing event IDs using the write connection for transaction consistency."""
        # This check is also performed inside _sync_events within the SAVEPOINT,
        # but the stream logic calls it beforehand for a pre-check.
        return await _find_existing_ids(self.write_conn, self.stream_id, event_ids)

    async def get_events(
        self, start_version: int = 0, event_filter: EventFilter | None = None
    ) -> AsyncIterable[StoredEvent]:
        """Gets events using a read connection."""
        async with self._get_read_conn() as conn:
            async for event in _get_events(
                conn, self.stream_id, start_version, event_filter
            ):
                yield event

    async def get_last_timestamp(self) -> datetime | None:
        """Gets the last timestamp using a read connection."""
        async with self._get_read_conn() as conn:
            return await _get_last_timestamp(conn, self.stream_id)

    async def save_snapshot(self, snapshot: Snapshot) -> int:
        """Saves a snapshot using the dedicated write connection."""
        async with self.write_lock:
            await _save_snapshot(self.write_conn, snapshot)
            return snapshot.version

    async def load_latest_snapshot(
        self, stream_id: str, projection_name: str = "default"
    ) -> Snapshot | None:
        """Loads a snapshot using a read connection."""
        async with self._get_read_conn() as conn:
            return await _load_latest_snapshot(conn, stream_id, projection_name)

    async def close(self):
        # Connections are managed by the factory, so the handle should not close them.
        pass
