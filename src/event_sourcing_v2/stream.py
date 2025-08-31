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
    AsyncContextManager,
    AsyncIterable,
    Dict,
    List
)
from datetime import datetime, timezone

from .models import Event, Snapshot
from .protocols import Notifier, StorageHandle, Stream
from .adaptors.sqlite import sqlite_open_adapter, SQLitePollingNotifier
import urllib.parse

class StreamImpl(Stream):
    def __init__(
        self, config: Dict, stream_id: str, handle: StorageHandle, notifier: Notifier
    ):
        self.config = config
        self.stream_id = stream_id
        self.handle = handle
        self.notifier = notifier
        self.version = self.handle.version

    async def write(self, events: List[Event], expected_version: int = -1) -> int:
        if not all(isinstance(e, Event) for e in events):
            raise TypeError("All items in events list must be Event objects")

        effective_expected_version = (
            self.version if expected_version == -1 else expected_version
        )

        idempotency_keys_to_check = [event.id for event in events if event.id]
        existing_ids = await self.handle.find_existing_ids(idempotency_keys_to_check)

        new_events_to_persist = []
        for event in events:
            if not event.id or event.id not in existing_ids:
                new_events_to_persist.append(event)

        await self.handle.sync(new_events_to_persist, effective_expected_version)

        self.version += len(new_events_to_persist)
        self.handle.version = self.version

        return self.version

    async def snapshot(self, state: bytes, projection_name: str = "default"):
        # Compress the state before saving to reduce storage and I/O.
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
                # Decompress the state. Return a new snapshot with the decompressed data.
                decompressed_state = zlib.decompress(snapshot.state)
                return snapshot.model_copy(update={"state": decompressed_state})
            except zlib.error:
                # For backward compatibility, if decompression fails,
                # assume it's an old, uncompressed snapshot.
                return snapshot
        return None

    async def read(self, from_version: int = 0) -> AsyncIterable[Event]:
        async for event in self.handle.get_events(start_version=from_version):
            yield event

    async def watch(self, from_version: int | None = None) -> AsyncIterable[Event]:
        # If from_version is not specified, default to the current version of the stream,
        # meaning we only watch for new events.
        effective_from_version = self.version if from_version is None else from_version
        queue = await self.notifier.subscribe(self.stream_id)
        last_yielded_version = effective_from_version

        try:
            # 1. Yield historical events
            async for event in self.handle.get_events(
                start_version=effective_from_version
            ):
                yield event
                last_yielded_version = event.version

            # 2. Drain the queue of any events that arrived during historical replay
            while not queue.empty():
                event = queue.get_nowait()
                if event.version > last_yielded_version:
                    yield event
                    last_yielded_version = event.version

            # 3. Watch for new live events
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
    """
    An async context manager that provides a configured `open_stream` function.

    This factory sets up shared resources (like database connections and notifiers)
    based on the provided configuration. It yields a function to open individual
    streams and ensures that all resources are gracefully cleaned up upon exit.

    Args:
        config: A dictionary containing configuration, like the database URL.

    Yields:
        A function `open_stream(stream_id)` that acts as an async context manager
        for a specific stream.

    Usage:
        async with stream_factory(config) as open_stream:
            async with open_stream("my_stream") as stream:
                ...
    """
    notifiers: Dict[str, Notifier] = {}
    connections: Dict[str, aiosqlite.Connection] = {}
    notifier_lock = asyncio.Lock()
    connection_lock = asyncio.Lock()

    async def cleanup():
        """
        Stops all active notifiers and closes all shared connections.
        This should be called before the application exits to ensure graceful shutdown.
        """
        async with notifier_lock, connection_lock:
            notifier_tasks = [notifier.stop() for notifier in notifiers.values()]
            connection_tasks = [conn.close() for conn in connections.values()]
            
            await asyncio.gather(*notifier_tasks, *connection_tasks)
            notifiers.clear()
            connections.clear()

    @asynccontextmanager
    async def open_stream(stream_id: str) -> AsyncContextManager[Stream]:
        # If no URL is provided, default to an in-memory SQLite database.
        url = config.get('url')
        if not url:
            url = 'sqlite://'

        scheme = url.split('://', 1)[0] if '://' in url else ''

        if not scheme == 'sqlite':
            raise ValueError(f"Unsupported scheme: {scheme}. Only 'sqlite' is supported.")

        parsed = urllib.parse.urlparse(url)
        db_path = parsed.path or parsed.netloc
        if not db_path or db_path == '/': # Handle sqlite:///
            db_path = ':memory:'

        polling_interval = config.get('polling_interval', 0.2) # Get from config, default to 0.2

        async with connection_lock:
            if db_path not in connections:
                connections[db_path] = await aiosqlite.connect(db_path)
        connection = connections[db_path]

        async with notifier_lock:
            if db_path not in notifiers:
                # Pass the shared connection to the notifier
                notifiers[db_path] = SQLitePollingNotifier(connection, polling_interval=polling_interval)
                await notifiers[db_path].start()
        notifier = notifiers[db_path]
        
        open_adapter = sqlite_open_adapter
        
        async with open_adapter(connection, db_path, stream_id) as handle:
            stream = StreamImpl(config, stream_id, handle, notifier)
            yield stream

    try:
        yield open_stream
    finally:
        await cleanup()
