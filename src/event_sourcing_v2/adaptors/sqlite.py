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
from ..stream import StreamImpl

from ..models import Event, Snapshot
from ..protocols import StorageHandle, Notifier, Stream


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

    async def sync(self, new_events: List[Event], expected_version: int):
        """Persists new events using the dedicated write connection."""
        async with self.write_lock:
            async with sqlite_open_adapter(self.write_conn, self.stream_id) as handle:
                new_version = await handle.sync(new_events, expected_version)
                self.version = new_version
                return new_version

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """Finds existing event IDs using the write connection, to ensure transaction consistency."""
        handle = SQLiteHandle(self.write_conn, self.stream_id)
        return await handle.find_existing_ids(event_ids)

    async def get_events(self, start_version: int = 0) -> AsyncIterable[Event]:
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
        async with self.conn.execute(
            "SELECT MAX(version) FROM events WHERE stream_id = ?", (self.stream_id,)
        ) as cursor:
            row = await cursor.fetchone()
            self.version = row[0] if row and row[0] is not None else 0

    async def sync(self, new_events: List[Event], expected_version: int):
        """
        Persist new events to the SQLite database in a transaction,
        atomically checking for the expected version.
        """
        # Filter out duplicates within the batch first
        if new_events:
            seen_ids = set()
            unique_new_events = []
            for e in new_events:
                if e.id:
                    if e.id not in seen_ids:
                        unique_new_events.append(e)
                        seen_ids.add(e.id)
                else:
                    unique_new_events.append(e)  # Events without ids are always unique
            new_events = unique_new_events
            
        # Idempotency check must be done inside the transaction.
        idempotency_keys_to_check = [e.id for e in new_events if e.id]
        if idempotency_keys_to_check:
            existing_ids = await self.find_existing_ids(idempotency_keys_to_check)
            new_events = [e for e in new_events if e.id not in existing_ids]

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
            idempotency_keys_to_check = [e.id for e in new_events if e.id]
            if idempotency_keys_to_check:
                existing_ids = await self.find_existing_ids(idempotency_keys_to_check)
                new_events = [e for e in new_events if e.id not in existing_ids]

            # 4. Insert new events if any
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
                            last_id_in_batch = _id # Update temporary variable
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


@asynccontextmanager
async def sqlite_stream_factory(
    db_path: str,
    *,
    cache_size_kib: int = -16384,
    polling_interval: float = 0.2,
    pool_size: int = 10,
) -> AsyncIterable[Stream]:
    """
    A factory for creating and managing streams that are backed by a SQLite database.
    This function is a Higher-Order Function that, when used as an async context
    manager, yields an `open_stream` function for creating individual stream instances.
    """
    notifiers: Dict[str, Notifier] = {}
    notifier_connections: Dict[str, aiosqlite.Connection] = {}
    write_connections: Dict[str, aiosqlite.Connection] = {}
    read_connection_pools: Dict[str, asyncio.Queue] = {}
    db_init_locks: Dict[str, asyncio.Lock] = {}
    db_init_locks_creator_lock = asyncio.Lock()
    write_locks: Dict[str, asyncio.Lock] = {}

    async def _initialize_db_resources(
        db_key: str, db_connect_string: str, is_memory_db: bool
    ):
        """Atomically initialize all database resources."""
        if db_key in read_connection_pools:
            return

        async with db_init_locks_creator_lock:
            if db_key not in db_init_locks:
                db_init_locks[db_key] = asyncio.Lock()

        db_lock = db_init_locks[db_key]
        async with db_lock:
            if db_key in read_connection_pools:
                return

            # cache_size_kib = config.get("cache_size_kib", -16384)  # Default to 16MB
            # polling_interval = config.get("polling_interval", 0.2)
            notifier_conn = await aiosqlite.connect(
                db_connect_string, uri=is_memory_db
            )
            await notifier_conn.execute("PRAGMA journal_mode=WAL;")
            await notifier_conn.execute("PRAGMA synchronous = NORMAL;")
            await notifier_conn.execute(f"PRAGMA cache_size = {cache_size_kib};")
            await notifier_conn.execute("PRAGMA busy_timeout = 5000;")
            notifier = SQLiteNotifier(notifier_conn, polling_interval=polling_interval)
            await notifier.start()
            notifiers[db_key] = notifier
            notifier_connections[db_key] = notifier_conn

            write_conn = await aiosqlite.connect(db_connect_string, uri=is_memory_db)
            await write_conn.execute("PRAGMA journal_mode=WAL;")
            await write_conn.execute("PRAGMA synchronous = NORMAL;")
            await write_conn.execute(f"PRAGMA cache_size = {cache_size_kib};")
            await write_conn.execute("PRAGMA busy_timeout = 5000;")
            write_connections[db_key] = write_conn
            write_locks[db_key] = asyncio.Lock()

            # pool_size = config.get("pool_size", 10)
            pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(
                maxsize=pool_size
            )
            read_connect_string = (
                f"file:{db_connect_string}?mode=ro"
                if not is_memory_db
                else db_connect_string
            )
            for _ in range(pool_size):
                conn = await aiosqlite.connect(read_connect_string, uri=True)
                await conn.execute(f"PRAGMA cache_size = {cache_size_kib};")
                await conn.execute("PRAGMA busy_timeout = 5000;")
                await pool.put(conn)
            read_connection_pools[db_key] = pool

    async def cleanup():
        """Stops all notifiers and closes all database connections."""
        notifier_tasks = [notifier.stop() for notifier in notifiers.values()]
        await asyncio.gather(*notifier_tasks)

        connection_tasks = []
        for conn in notifier_connections.values():
            connection_tasks.append(conn.close())
        for conn in write_connections.values():
            connection_tasks.append(conn.close())
        for pool in read_connection_pools.values():
            while not pool.empty():
                conn = await pool.get()
                connection_tasks.append(conn.close())
        await asyncio.gather(*connection_tasks)

    @asynccontextmanager
    async def open_stream(stream_id: str) -> AsyncIterable[Stream]:
        # db_path = config.get("db_path")
        if not db_path:
            raise ValueError("`db_path` must be provided in the configuration.")

        is_memory_db = db_path == ":memory:"

        if is_memory_db:
            db_key = ":memory:"
            db_connect_string = "file:memdb_shared?mode=memory&cache=shared"
        else:
            db_key = db_path
            db_connect_string = db_path

        await _initialize_db_resources(db_key, db_connect_string, is_memory_db)

        handle = SQLiteStorageHandle(
            stream_id=stream_id,
            write_conn=write_connections[db_key],
            write_lock=write_locks[db_key],
            read_pool=read_connection_pools[db_key],
        )
        stream = StreamImpl(
            stream_id=stream_id,
            handle=handle,
            notifier=notifiers[db_key],
        )
        await stream._async_init()
        yield stream

    try:
        yield open_stream
    finally:
        await cleanup()
