"""
This module implements the factory pattern for creating and managing event streams.

The `create_stream_factory` Higher-Order Function is the heart of the resource
management system. It captures shared resources (database connections, notifiers)
for a given configuration and returns an `open_stream` function that is bound
to those resources. This functional approach avoids global singletons and makes
resource management explicit.
"""
import asyncio
from contextlib import asynccontextmanager
import zlib
import aiosqlite
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
)
from datetime import datetime, timezone

from .models import Event, Snapshot
from .protocols import Notifier, StorageHandle, Stream
from .adaptors.sqlite import (
    sqlite_open_adapter,
    SQLiteNotifier,
    sqlite_read_adapter,
)


class StreamImpl(Stream):
    def __init__(self, stream_id: str, handle: StorageHandle, notifier: Notifier):
        self.stream_id = stream_id
        self.handle = handle
        self.notifier = notifier
        self.version = -1  # Uninitialized

    async def _async_init(self):
        """Asynchronously initializes the stream's version."""
        await self.handle._async_init()
        self.version = self.handle.version

    async def write(self, events: List[Event], expected_version: int = -1) -> int:
        if not all(isinstance(e, Event) for e in events):
            raise TypeError("All items in events list must be Event objects")

        effective_expected_version = (
            self.version if expected_version == -1 else expected_version
        )
        
        new_version = await self.handle.sync(events, effective_expected_version)
        self.version = new_version
        return self.version

    async def snapshot(self, state: bytes, projection_name: str = "default"):
        compressed_state = zlib.compress(state)
        snapshot = Snapshot(
            stream_id=self.stream_id,
            projection_name=projection_name,
            version=self.version,
            state=compressed_state,
            timestamp=datetime.now(timezone.utc),
        )
        await self.handle.save_snapshot(snapshot)

    async def load_snapshot(self, projection_name: str = "default") -> Snapshot | None:
        snapshot = await self.handle.load_latest_snapshot(
            self.stream_id, projection_name=projection_name
        )
        if snapshot:
            try:
                decompressed_state = zlib.decompress(snapshot.state)
                return snapshot.model_copy(update={"state": decompressed_state})
            except zlib.error:
                return snapshot
        return None

    async def read(self, from_version: int = 0) -> AsyncIterable[Event]:
        async for event in self.handle.get_events(start_version=from_version):
            yield event

    async def watch(self, from_version: int | None = None) -> AsyncIterable[Event]:
        effective_from_version = self.version if from_version is None else from_version
        queue = await self.notifier.subscribe(self.stream_id)
        last_yielded_version = effective_from_version

        try:
            async for event in self.handle.get_events(
                start_version=effective_from_version
            ):
                yield event
                last_yielded_version = event.version

            while not queue.empty():
                event = queue.get_nowait()
                if event.version > last_yielded_version:
                    yield event
                    last_yielded_version = event.version

            while True:
                event = await queue.get()
                if event.version > last_yielded_version:
                    yield event
                    last_yielded_version = event.version
        finally:
            await self.notifier.unsubscribe(self.stream_id, queue)

    async def metrics(self) -> Dict[str, Any]:
        last_ts = await self.handle.get_last_timestamp()
        return {
            "current_version": self.version,
            "event_count": self.version,
            "last_timestamp": last_ts,
        }


@asynccontextmanager
async def stream_factory(config: Dict):
    notifiers: Dict[str, Notifier] = {}
    notifier_connections: Dict[str, aiosqlite.Connection] = {}
    write_connections: Dict[str, aiosqlite.Connection] = {}
    read_connection_pools: Dict[str, asyncio.Queue] = {}
    db_init_locks: Dict[str, asyncio.Lock] = {}
    db_init_locks_creator_lock = asyncio.Lock()

    async def _initialize_db_resources(
        db_key: str, db_connect_string: str, is_memory_db: bool, config: Dict
    ):
        """Atomically initialize all database resources (notifier, write conn, read pool)."""
        if db_key in read_connection_pools:
            return

        async with db_init_locks_creator_lock:
            if db_key not in db_init_locks:
                db_init_locks[db_key] = asyncio.Lock()

        db_lock = db_init_locks[db_key]
        async with db_lock:
            if db_key in read_connection_pools:
                return

            # 1. Create Notifier and its dedicated connection.
            # This also handles creating the schema.
            polling_interval = config.get("polling_interval", 0.2)
            notifier_conn = await aiosqlite.connect(
                db_connect_string, uri=is_memory_db
            )
            await notifier_conn.execute("PRAGMA journal_mode=WAL;")
            notifier = SQLiteNotifier(notifier_conn, polling_interval=polling_interval)
            await notifier.start()
            notifiers[db_key] = notifier
            notifier_connections[db_key] = notifier_conn

            # 2. Create the single, dedicated write connection.
            write_conn = await aiosqlite.connect(db_connect_string, uri=is_memory_db)
            await write_conn.execute("PRAGMA journal_mode=WAL;")
            write_connections[db_key] = write_conn

            # 3. Create the pool of read-only connections.
            pool_size = config.get("pool_size", 10)
            pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(maxsize=pool_size)
            # For in-memory, we MUST use the same URI to connect to the same DB.
            # For file-based, we can use mode=ro for safety.
            read_connect_string = (
                f"file:{db_connect_string}?mode=ro"
                if not is_memory_db
                else db_connect_string
            )
            for _ in range(pool_size):
                conn = await aiosqlite.connect(read_connect_string, uri=True)
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
        db_path = config.get("db_path")
        if not db_path:
            raise ValueError("`db_path` must be provided in the configuration.")

        is_memory_db = db_path == ":memory:"

        if is_memory_db:
            db_key = ":memory:"
            db_connect_string = "file:memdb_shared?mode=memory&cache=shared"
        else:
            db_key = db_path
            db_connect_string = db_path

        await _initialize_db_resources(
            db_key, db_connect_string, is_memory_db, config
        )

        # Create a StorageHandle based on the configuration
        # This is a placeholder; in a real application, you'd instantiate
        # the appropriate adaptor (SQLiteNotifier, SQLiteReadAdapter, etc.)
        # and pass it to the StreamImpl constructor.
        # For now, we'll just pass a dummy handle.
        # In a real scenario, you'd do something like:
        # from .adaptors.sqlite import SQLiteNotifier, SQLiteReadAdapter
        # handle = SQLiteNotifier(write_connections[db_key], polling_interval=0.2)
        # await handle._async_init() # Initialize the handle's version
        handle = StorageHandle(write_connections[db_key], read_connection_pools[db_key])

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
