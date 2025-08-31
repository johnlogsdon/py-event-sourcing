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
import aiosqlite
from typing import (
    Any,
    AsyncContextManager,
    AsyncIterable,
    Awaitable,
    Callable,
    Dict,
    List,
    Set,
    Tuple,
)
from datetime import datetime, timezone

from .models import Event, Snapshot
from .protocols import Notifier, StorageHandle, Stream
from .adaptors.sqlite import sqlite_open_adapter, SQLitePollingNotifier
import os
import urllib.parse


class StreamImpl(Stream):
    """
    Represents a single, versioned stream of events.

    This class provides the core API for writing, reading, and watching events,
    as well as creating and loading snapshots. It is designed to be "lazy,"
    meaning it doesn't load all events into memory upon initialization. Instead,
    it fetches the current version number and retrieves events from storage on demand.
    """

    def __init__(
        self, config: Dict, stream_id: str, handle: StorageHandle, notifier: Notifier
    ):
        self.config = config
        self.stream_id = stream_id
        self.handle = handle
        self.notifier = notifier
        # The stream is now "lazy" and doesn't load events into memory on startup.
        # The version is fetched from the handle, which gets it from a COUNT query.
        # This is much more memory-efficient for large streams.
        self.version = self.handle.version

    async def write(self, events: List[Event], expected_version: int = -1) -> int:
        """
        Appends a list of events to the stream atomically.

        This operation is idempotent based on the `id` field of each event. If an
        event with the same `id` has already been persisted, it is ignored.

        Args:
            events: A list of `Event` objects to append.
            expected_version: If specified, the write will only succeed if the
                current stream version matches this value. Use -1 (the default)
                to bypass this check.
        """
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

    async def snapshot(self, state: bytes):
        """
        Saves a snapshot of the stream's state at the current version.

        Snapshots are an optimization to speed up state reconstruction. Instead of
        replaying all events from the beginning, an application can load the latest
        snapshot and then replay only the events that occurred after that snapshot.
        """
        snapshot = Snapshot(
            stream_id=self.stream_id,
            version=self.version,
            state=state,
            timestamp=datetime.now(timezone.utc),
        )
        await self.handle.save_snapshot(snapshot)

    async def load_snapshot(self) -> Snapshot | None:
        """Loads the latest snapshot for the stream, or None if no snapshot exists."""
        return await self.handle.load_latest_snapshot(self.stream_id)

    async def read(self, from_version: int = 0) -> AsyncIterable[Event]:
        """
        Returns an async generator that yields historical events from the stream,
        starting from a specific version.

        Args:
            from_version: The version *after* which to start reading events.
                Defaults to 0, which reads from the beginning of the stream.
        """
        async for event in self.handle.get_events(start_version=from_version):
            yield event

    async def watch(self, from_version: int | None = None) -> AsyncIterable[Event]:
        """
        Returns an async generator that first yields historical events from the given
        version and then continues to yield new, live events as they are appended.

        This method is designed to be race-condition-free. It replays historical
        events, drains a temporary queue of any events that arrived during replay,
        and then switches to listening for live events from the notifier.

        Args:
            from_version: The version *after* which to start watching for events.
                Defaults to the current stream version (watches for new events only).
                To replay all events from the beginning, pass `from_version=0`.
        """
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
        """Returns a dictionary of key metrics for the stream."""
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

        if scheme == 'sqlite':
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
        else:
            raise ValueError(f"Unsupported scheme: {scheme}. Only 'sqlite' is supported.")

        async with open_adapter(connection, db_path, stream_id) as handle:
            stream = StreamImpl(config, stream_id, handle, notifier)
            yield stream

    try:
        yield open_stream
    finally:
        await cleanup()
