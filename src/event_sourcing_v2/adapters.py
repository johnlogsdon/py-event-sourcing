from typing import AsyncIterable, AsyncIterator, List, Set
from contextlib import asynccontextmanager
import aiosqlite
import asyncio
import json
import logging
import pydantic_core
import urllib.parse
import os
from datetime import datetime

from .models import Event


class SQLiteHandle:
    def __init__(self, conn: aiosqlite.Connection, stream_id: str, initial_version: int):
        self.conn = conn
        self.stream_id = stream_id
        self.version = initial_version

    async def sync(self, new_events: List[Event]):
        """Persist new events to the SQLite database in a transaction."""
        if not new_events:
            return

        params = [
            (
                event.type,
                event.timestamp.isoformat(),
                json.dumps(event.metadata),
                event.data,
            )
            for event in new_events
        ]
        try:
            # Manually handle transaction to ensure compatibility with different
            # aiosqlite versions, as `async with conn` has changed behavior.
            await self.conn.execute("BEGIN")
            await self.conn.executemany(
                """
                INSERT INTO events (stream_id, event_type, timestamp, metadata, data)
                VALUES (?, ?, ?, ?, ?)
                """,
                [(self.stream_id, *p) for p in params],
            )
            await self.conn.commit()
        except Exception as e:
            await self.conn.rollback()
            logging.error(f"Failed to sync events to SQLite: {e}")
            raise

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """Given a list of event IDs, query the DB and return the set that already exist."""
        if not event_ids:
            return set()
        placeholders = ",".join("?" for _ in event_ids)
        query = f"SELECT json_extract(metadata, '$.id') FROM events WHERE stream_id = ? AND json_extract(metadata, '$.id') IN ({placeholders})"
        params = [self.stream_id] + event_ids
        existing_ids = set()
        async with self.conn.execute(query, params) as cursor:
            async for row in cursor:
                existing_ids.add(row[0])
        return existing_ids

    async def get_events(self) -> AsyncIterable[Event]:
        """An async generator to stream events from the database for a given stream_id."""
        async with self.conn.execute(
            "SELECT event_type, timestamp, metadata, data FROM events WHERE stream_id = ? ORDER BY id",
            (self.stream_id,),
        ) as cursor:
            async for row in cursor:
                event_type, timestamp_str, metadata_json, data_blob = row
                try:
                    yield Event(
                        type=event_type,
                        timestamp=datetime.fromisoformat(timestamp_str),
                        metadata=json.loads(metadata_json),
                        data=data_blob,
                    )
                except (json.JSONDecodeError, pydantic_core.ValidationError, KeyError, ValueError) as e:
                    logging.warning(f"Skipping invalid event row during replay for stream {self.stream_id}: {e}")

    async def close(self):
        # The connection is now shared and managed by the factory, so the handle should not close it.
        pass


@asynccontextmanager
async def sqlite_open_adapter(conn: aiosqlite.Connection, db_path: str, stream_id: str) -> AsyncIterator[SQLiteHandle]:
    """Adapter that uses a shared database connection."""
    # The Notifier also creates the table, but we do it here as well to ensure
    # it exists before the first SELECT, and to handle the case of multiple
    # connections to an in-memory database. It's idempotent.
    await conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            stream_id TEXT NOT NULL,
            event_type TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            metadata TEXT NOT NULL,
            data BLOB NOT NULL
        )
    """)

    # Get the initial version count for the stream.
    async with conn.execute(
        "SELECT COUNT(id) FROM events WHERE stream_id = ?",
        (stream_id,),
    ) as cursor:
        row = await cursor.fetchone()
        initial_version = row[0] if row else 0

    handle = SQLiteHandle(conn, stream_id, initial_version)
    try:
        yield handle
    finally:
        await handle.close()
