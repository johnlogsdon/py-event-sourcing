import asyncio
from contextlib import asynccontextmanager
from typing import Dict, AsyncIterable

import aiosqlite

from .models import Event, Snapshot
from .protocols import Notifier, Stream
from .stream import StreamImpl
from .adaptors.sqlite import SQLiteNotifier, SQLiteStorageHandle


class SQLiteStorage:
    """
    A factory for creating and managing streams that are backed by a SQLite database.
    This class manages all database resources, including connection pools and notifiers.
    """

    def __init__(self, config: Dict):
        self.config = config
        self.notifiers: Dict[str, Notifier] = {}
        self.notifier_connections: Dict[str, aiosqlite.Connection] = {}
        self.write_connections: Dict[str, aiosqlite.Connection] = {}
        self.read_connection_pools: Dict[str, asyncio.Queue] = {}
        self.db_init_locks: Dict[str, asyncio.Lock] = {}
        self.db_init_locks_creator_lock = asyncio.Lock()
        self.write_locks: Dict[str, asyncio.Lock] = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.cleanup()

    async def _initialize_db_resources(
        self, db_key: str, db_connect_string: str, is_memory_db: bool
    ):
        """Atomically initialize all database resources."""
        if db_key in self.read_connection_pools:
            return

        async with self.db_init_locks_creator_lock:
            if db_key not in self.db_init_locks:
                self.db_init_locks[db_key] = asyncio.Lock()

        db_lock = self.db_init_locks[db_key]
        async with db_lock:
            if db_key in self.read_connection_pools:
                return

            polling_interval = self.config.get("polling_interval", 0.2)
            notifier_conn = await aiosqlite.connect(
                db_connect_string, uri=is_memory_db
            )
            await notifier_conn.execute("PRAGMA journal_mode=WAL;")
            notifier = SQLiteNotifier(notifier_conn, polling_interval=polling_interval)
            await notifier.start()
            self.notifiers[db_key] = notifier
            self.notifier_connections[db_key] = notifier_conn

            write_conn = await aiosqlite.connect(db_connect_string, uri=is_memory_db)
            await write_conn.execute("PRAGMA journal_mode=WAL;")
            self.write_connections[db_key] = write_conn
            self.write_locks[db_key] = asyncio.Lock()

            # 3. Create the pool of read-only connections.
            pool_size = self.config.get("pool_size", 10)
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
                await pool.put(conn)
            self.read_connection_pools[db_key] = pool

    async def cleanup(self):
        """Stops all notifiers and closes all database connections."""
        notifier_tasks = [notifier.stop() for notifier in self.notifiers.values()]
        await asyncio.gather(*notifier_tasks)

        connection_tasks = []
        for conn in self.notifier_connections.values():
            connection_tasks.append(conn.close())
        for conn in self.write_connections.values():
            connection_tasks.append(conn.close())
        for pool in self.read_connection_pools.values():
            while not pool.empty():
                conn = await pool.get()
                connection_tasks.append(conn.close())

        await asyncio.gather(*connection_tasks)

    @asynccontextmanager
    async def open_stream(self, stream_id: str) -> AsyncIterable[Stream]:
        db_path = self.config.get("db_path")
        if not db_path:
            raise ValueError("`db_path` must be provided in the configuration.")

        is_memory_db = db_path == ":memory:"

        if is_memory_db:
            db_key = ":memory:"
            db_connect_string = "file:memdb_shared?mode=memory&cache=shared"
        else:
            db_key = db_path
            db_connect_string = db_path

        await self._initialize_db_resources(db_key, db_connect_string, is_memory_db)

        handle = SQLiteStorageHandle(
            stream_id=stream_id,
            write_conn=self.write_connections[db_key],
            write_lock=self.write_locks[db_key],
            read_pool=self.read_connection_pools[db_key],
        )
        stream = StreamImpl(
            stream_id=stream_id,
            handle=handle,
            notifier=self.notifiers[db_key],
        )
        await stream._async_init()
        yield stream
