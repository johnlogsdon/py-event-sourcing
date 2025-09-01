"""
This module implements the factory pattern for creating and managing event streams.

The `create_stream_factory` Higher-Order Function is the heart of the resource
management system. It captures shared resources (database connections, notifiers)
for a given configuration and returns an `open_stream` function that is bound
to those resources. This functional approach avoids global singletons and makes
resource management explicit.
"""
import zlib
from typing import (
    Any,
    AsyncIterable,
    Dict,
    List,
)
from datetime import datetime, timezone

from .models import Event, Snapshot
from .protocols import Notifier, StorageHandle, Stream

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
