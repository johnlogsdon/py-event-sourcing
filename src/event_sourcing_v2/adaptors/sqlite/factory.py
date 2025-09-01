from typing import AsyncIterable, Dict
from contextlib import asynccontextmanager
import aiosqlite
import asyncio
from event_sourcing_v2.stream import StreamImpl
from event_sourcing_v2.protocols import Notifier, Stream
from event_sourcing_v2.adaptors.sqlite.handle import SQLiteStorageHandle
from event_sourcing_v2.adaptors.sqlite.notifier import SQLiteNotifier


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
