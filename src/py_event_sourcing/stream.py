"""
This module implements the factory pattern for creating and managing event streams.

The `create_stream_factory` Higher-Order Function is the heart of the resource
management system. It captures shared resources (database connections, notifiers)
for a given configuration and returns an `open_stream` function that is bound
to those resources. This functional approach avoids global singletons and makes
resource management explicit.
"""

import zlib
from datetime import datetime, timezone
from typing import Any, AsyncIterable, Dict, List, Union

from .models import CandidateEvent, EventFilter, Snapshot, StoredEvent
from .protocols import Notifier, StorageHandle, Stream


class StreamImpl(Stream):
    """
    The concrete implementation of the `Stream` protocol.
    """

    def __init__(
        self,
        stream_id: str,
        storage_handle: StorageHandle,
        notifier: Notifier,
    ):
        self.stream_id = stream_id
        self.storage_handle = storage_handle
        self.notifier = notifier
        self.version = -1  # Uninitialized

    async def _async_init(self):
        """Initializes the stream by fetching its current version."""
        self.version = await self.storage_handle.get_version()

    async def write(
        self,
        events: Union[CandidateEvent, List[CandidateEvent]],
        expected_version: int = -1,
    ) -> int:
        """Appends one or more events to the stream."""
        if self.stream_id == "@all":
            raise ValueError("Writing to the '@all' stream is not permitted.")

        if not isinstance(events, list):
            events = [events]

        if not all(isinstance(e, CandidateEvent) for e in events):
            raise TypeError("All items in events list must be CandidateEvent objects")

        if not events:
            return self.version

        # Optimistic concurrency check before sending to storage
        if expected_version != -1 and self.version != expected_version:
            raise ValueError(
                f"Concurrency conflict: expected version {expected_version}, but stream is at {self.version}"
            )

        # Idempotency pre-check
        idempotency_keys_to_check = [
            e.idempotency_key for e in events if e.idempotency_key
        ]
        if idempotency_keys_to_check:
            existing_ids = await self.storage_handle.find_existing_ids(
                idempotency_keys_to_check
            )
            events = [e for e in events if e.idempotency_key not in existing_ids]

        if not events:
            return self.version

        self.version = await self.storage_handle.sync(events, expected_version)
        return self.version

    async def snapshot(
        self, state: bytes, version: int | None = None, projection_name: str = "default"
    ):
        """Saves a snapshot of the stream's state."""
        snapshot_version = version if version is not None else self.version

        # For the @all stream, the version is the global sequence ID of the last event processed.
        # Ensure the user provides it explicitly.
        if self.stream_id == "@all" and version is None:
            raise ValueError(
                "A version (representing the global sequence ID) must be provided for @all stream snapshots."
            )

        snapshot = Snapshot(
            stream_id=self.stream_id,
            projection_name=projection_name,
            version=snapshot_version,
            state=state,
            timestamp=datetime.utcnow(),
        )
        self.version = await self.storage_handle.save_snapshot(snapshot)

    async def load_snapshot(self, projection_name: str = "default") -> Snapshot | None:
        """Loads the latest snapshot for the stream."""
        snapshot = await self.storage_handle.load_latest_snapshot(
            self.stream_id, projection_name=projection_name
        )
        if snapshot:
            try:
                decompressed_state = zlib.decompress(snapshot.state)
                return snapshot.model_copy(update={"state": decompressed_state})
            except zlib.error:
                return snapshot
        return None

    async def read(
        self, from_version: int = 0, event_filter: EventFilter | None = None
    ) -> AsyncIterable[StoredEvent]:
        async for event in self.storage_handle.get_events(
            start_version=from_version, event_filter=event_filter
        ):
            yield event

    async def watch(
        self, from_version: int | None = None
    ) -> AsyncIterable[StoredEvent]:
        effective_from_version = self.version if from_version is None else from_version
        queue = await self.notifier.subscribe(self.stream_id)
        last_yielded_version = effective_from_version

        try:
            async for event in self.storage_handle.get_events(
                start_version=effective_from_version
            ):
                yield event
                last_yielded_version = (
                    event.sequence_id if self.stream_id == "@all" else event.version
                )

            while not queue.empty():
                event = queue.get_nowait()
                current_event_version = (
                    event.sequence_id if self.stream_id == "@all" else event.version
                )
                if current_event_version > last_yielded_version:
                    yield event
                    last_yielded_version = current_event_version

            while True:
                event = await queue.get()
                current_event_version = (
                    event.sequence_id if self.stream_id == "@all" else event.version
                )
                if current_event_version > last_yielded_version:
                    yield event
                    last_yielded_version = current_event_version
        finally:
            await self.notifier.unsubscribe(self.stream_id, queue)

    async def metrics(self) -> Dict[str, Any]:
        last_ts = await self.storage_handle.get_last_timestamp()
        return {
            "current_version": self.version,
            "event_count": self.version,
            "last_timestamp": last_ts,
        }
