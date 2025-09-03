from contextlib import asynccontextmanager
from typing import AsyncContextManager, AsyncIterator, Callable

from py_event_sourcing.protocols import Stream
from py_event_sourcing.stream import StreamImpl

from .handle import SQLiteStorageHandle
from .resource_manager import SQLiteResourceManager


@asynccontextmanager
async def sqlite_stream_factory(
    db_path: str,
    *,
    cache_size_kib: int = -16384,
    polling_interval: float = 0.2,
    pool_size: int = 10,
) -> AsyncIterator[Callable[[str], AsyncContextManager[Stream]]]:
    """
    A factory for creating and managing streams that are backed by a SQLite database.
    This function is a Higher-Order Function that, when used as an async context
    manager, yields an `open_stream` function for creating individual stream instances.
    """
    resource_manager = SQLiteResourceManager()

    @asynccontextmanager
    async def open_stream(stream_id: str) -> AsyncIterator[Stream]:
        if not db_path:
            raise ValueError("`db_path` must be provided in the configuration.")

        resources = await resource_manager.get_resources(
            db_path,
            cache_size_kib=cache_size_kib,
            polling_interval=polling_interval,
            pool_size=pool_size,
        )

        handle = SQLiteStorageHandle(
            stream_id=stream_id,
            write_conn=resources.write_conn,
            write_lock=resources.write_lock,
            read_pool=resources.read_pool,
        )
        stream = StreamImpl(
            stream_id=stream_id,
            storage_handle=handle,
            notifier=resources.notifier,
        )
        await stream._async_init()
        yield stream

    try:
        yield open_stream
    finally:
        await resource_manager.close_all()
