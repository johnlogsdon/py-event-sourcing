import aiosqlite
import asyncio
from typing import Dict, NamedTuple

from .notifier import SQLiteNotifier


class DBResources(NamedTuple):
    """A container for all resources related to a single database file."""
    write_conn: aiosqlite.Connection
    write_lock: asyncio.Lock
    read_pool: asyncio.Queue
    notifier: SQLiteNotifier


class SQLiteResourceManager:
    """
    Manages the lifecycle of SQLite resources (connections, pools, notifiers)
    for the entire process.
    """

    def __init__(self):
        self._resources: Dict[str, DBResources] = {}
        self._db_init_locks: Dict[str, asyncio.Lock] = {}
        self._creator_lock = asyncio.Lock()

    async def get_resources(
        self,
        db_path: str,
        *,
        cache_size_kib: int,
        polling_interval: float,
        pool_size: int,
    ) -> DBResources:
        """
        Atomically initializes and returns all database resources for a given path.
        This method is idempotent.
        """
        is_memory_db = db_path == ":memory:"
        db_key = ":memory:" if is_memory_db else db_path
        
        if db_key in self._resources:
            return self._resources[db_key]

        async with self._creator_lock:
            if db_key not in self._db_init_locks:
                self._db_init_locks[db_key] = asyncio.Lock()

        db_lock = self._db_init_locks[db_key]
        async with db_lock:
            if db_key in self._resources:
                return self._resources[db_key]

            db_connect_string = (
                "file:memdb_shared?mode=memory&cache=shared" if is_memory_db else db_path
            )

            # Create Write Connection and initialize schema first
            write_conn = await aiosqlite.connect(db_connect_string, uri=is_memory_db)
            await self._configure_connection(write_conn, cache_size_kib)
            await self._initialize_schema(write_conn)

            # Now, create Notifier and its connection
            notifier_conn = await aiosqlite.connect(db_connect_string, uri=is_memory_db)
            await self._configure_connection(notifier_conn, cache_size_kib)
            notifier = SQLiteNotifier(notifier_conn, polling_interval=polling_interval)
            await notifier.start()

            # Create Read Pool
            pool: asyncio.Queue[aiosqlite.Connection] = asyncio.Queue(maxsize=pool_size)
            read_connect_string = (
                f"file:{db_path}?mode=ro" if not is_memory_db else db_connect_string
            )
            for _ in range(pool_size):
                conn = await aiosqlite.connect(read_connect_string, uri=True)
                await conn.execute(f"PRAGMA cache_size = {cache_size_kib};")
                await conn.execute("PRAGMA busy_timeout = 5000;")
                await pool.put(conn)

            resources = DBResources(
                write_conn=write_conn,
                write_lock=asyncio.Lock(),
                read_pool=pool,
                notifier=notifier,
            )
            self._resources[db_key] = resources
            return resources

    async def _initialize_schema(self, conn: aiosqlite.Connection):
        """Creates the necessary tables and indexes if they don't exist."""
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stream_id TEXT NOT NULL,
                idempotency_key TEXT,
                event_type TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                metadata TEXT,
                data BLOB,
                version INTEGER NOT NULL,
                UNIQUE(stream_id, version),
                UNIQUE(stream_id, idempotency_key)
            )
        """
        )
        await conn.execute(
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
        await conn.commit()

    async def _configure_connection(self, conn: aiosqlite.Connection, cache_size_kib: int):
        """Applies standard PRAGMA settings to a connection."""
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA synchronous = NORMAL;")
        await conn.execute(f"PRAGMA cache_size = {cache_size_kib};")
        await conn.execute("PRAGMA busy_timeout = 5000;")

    async def close_all(self):
        """Stops all notifiers and closes all database connections."""
        all_resources = list(self._resources.values())
        self._resources.clear()

        notifier_tasks = [res.notifier.stop() for res in all_resources]
        await asyncio.gather(*notifier_tasks, return_exceptions=True)

        connection_tasks = []
        for res in all_resources:
            connection_tasks.append(res.notifier.conn.close())
            connection_tasks.append(res.write_conn.close())
            while not res.read_pool.empty():
                conn = await res.read_pool.get()
                connection_tasks.append(conn.close())
        
        await asyncio.gather(*connection_tasks, return_exceptions=True)


    def __del__(self):
        # Ensure cleanup happens if the object is garbage collected,
        # though explicit closing is preferred.
        if self._resources:
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self.close_all())
            except RuntimeError: # No running loop
                asyncio.run(self.close_all())
