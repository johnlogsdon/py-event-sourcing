"""
This module defines the abstract protocols for storage and notification.

By using `Protocol`-based interfaces, the core stream logic is decoupled from
the concrete implementation details of the backend. This makes the system
extensible, allowing for future support of different databases (e.g., PostgreSQL)
or notification systems (e.g., Redis Pub/Sub) without changing the `Stream` class.
This is a key principle of the library's design.
"""

import asyncio
from datetime import datetime
from typing import Any, AsyncIterable, Dict, List, Protocol, Set, Union

from .models import CandidateEvent, EventFilter, Snapshot, StoredEvent


class StorageHandle(Protocol):
    """
    Defines the contract that all storage adapters must implement.
    The Stream class interacts with this protocol, not a concrete implementation.
    """

    async def get_version(self) -> int:
        """
        Retrieves the latest version number for the stream.
        """
        ...

    async def sync(
        self, new_events: List[CandidateEvent], expected_version: int
    ) -> int:
        """
        Atomically persists a list of new events.

        This method should ensure that the write is atomic and respects the
        optimistic concurrency control via `expected_version`.

        Args:
            new_events: A list of `Event` objects to persist.
            expected_version: The version the stream is expected to be at. The
                write should fail if the actual version does not match.

        Returns:
            The new version number of the stream after the write operation.

        Raises:
            ValueError: If the `expected_version` does not match the current
                stream version in the storage.
        """
        ...

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        """
        Finds which of the given event IDs already exist in the storage.

        This is used to ensure idempotency of writes.

        Args:
            event_ids: A list of event IDs to check.

        Returns:
            A set of event IDs that are already present in the storage.
        """
        ...

    async def get_events(
        self, start_version: int = 0, event_filter: EventFilter | None = None
    ) -> AsyncIterable[StoredEvent]:
        """
        Retrieves events from the storage starting from a given version.

        Args:
            start_version: The version number to start retrieving events from.
                Defaults to 0 to retrieve all events.

        Yields:
            `Event` objects in order.
        """
        ...

    async def get_last_timestamp(self) -> datetime | None:
        """
        Retrieves the timestamp of the most recent event in the stream.

        Returns:
            The timestamp of the last event, or `None` if the stream is empty.
        """
        ...

    async def save_snapshot(self, snapshot: Snapshot) -> int:
        """
        Saves a snapshot of a stream's state.

        Args:
            snapshot: The `Snapshot` object to save.

        Returns:
            The version number of the saved snapshot.
        """
        ...

    async def load_latest_snapshot(
        self, stream_id: str, projection_name: str = "default"
    ) -> Snapshot | None:
        """
        Loads the most recent snapshot for a given stream.

        Args:
            stream_id: The ID of the stream for which to load the snapshot.
            projection_name: The name of the projection to load the snapshot for.

        Returns:
            The latest `Snapshot` object, or `None` if no snapshot exists.
        """
        ...

    async def close(self):
        """
        Closes the storage handle and releases any underlying resources.
        """
        ...


class Notifier(Protocol):
    """
    Defines the contract for notifying watchers of new events.
    This allows for different notification strategies (e.g., polling, pub/sub).
    """

    async def start(self):
        """
        Starts the notifier, e.g., by beginning a polling loop.
        """
        ...

    async def stop(self):
        """
        Stops the notifier and cleans up its resources.
        """
        ...

    async def subscribe(self, stream_id: str) -> asyncio.Queue[StoredEvent]:
        """
        Subscribes to a stream to receive notifications of new events.

        Args:
            stream_id: The ID of the stream to watch.

        Returns:
            An `asyncio.Queue` on which new `Event` objects will be placed.
        """
        ...

    async def unsubscribe(self, stream_id: str, queue: asyncio.Queue[StoredEvent]):
        """
        Unsubscribes a queue from a stream's notifications.

        Args:
            stream_id: The ID of the stream to unsubscribe from.
            queue: The queue that was previously returned by `subscribe`.
        """
        ...


class Stream(Protocol):
    """
    Defines the public interface for an event stream.
    This allows the factory to return a stream object without coupling the user
    to a specific implementation class.
    """

    version: int
    stream_id: str

    async def write(
        self,
        events: Union[CandidateEvent, List[CandidateEvent]],
        expected_version: int = -1,
    ) -> int:
        """
        Appends a list of events to the stream atomically.

        This operation is idempotent based on the `id` field of each event. If an
        event with the same `id` has already been persisted, it is ignored.

        Args:
            events: A list of `Event` objects to append.
            expected_version: If specified, the write will only succeed if the
                current stream version matches this value. Use -1 (the default)
                to bypass this check for unconditional writes.

        Returns:
            The new version number of the stream after the write.

        Raises:
            ValueError: If `expected_version` does not match the current stream version.
            TypeError: If any object in the `events` list is not an `Event` instance.
        """
        ...

    async def snapshot(
        self, state: bytes, version: int | None = None, projection_name: str = "default"
    ):
        """
        Saves a snapshot of the stream's state at the current version.

        Snapshots are an optimization to speed up state reconstruction. Instead of
        replaying all events from the beginning, an application can load the latest
        snapshot and then replay only the events that occurred after that snapshot.

        Args:
            state: The serialized state of the aggregate to be saved.
            version: The version at which the snapshot is taken. If not provided,
                defaults to the current stream version. For the '@all' stream, this
                should be the global sequence ID of the last event processed.
            projection_name: The name of the projection this snapshot is for.
                This allows a single event stream to have snapshots for multiple
                different read models.
        """
        ...

    async def load_snapshot(self, projection_name: str = "default") -> Snapshot | None:
        """
        Loads the latest snapshot for the stream.
        Args:
            projection_name: The name of the projection to load the snapshot for.

        Returns:
            A `Snapshot` object if one exists, otherwise `None`.
        """
        ...

    async def read(
        self, from_version: int = 0, event_filter: EventFilter | None = None
    ) -> AsyncIterable[StoredEvent]:
        """
        Returns an async generator that yields historical events from the stream.

        Args:
            from_version: The version from which to start reading events (inclusive).
                Defaults to 0, which reads from the beginning of the stream.
                For the special '@all' stream, this refers to the global sequence ID.

        Yields:
            `StoredEvent` objects from the stream in order.
        """
        ...

    async def watch(
        self, from_version: int | None = None
    ) -> AsyncIterable[StoredEvent]:
        """
        Returns an async generator that yields historical events and then live events.

        This method first yields historical events from the given version and then
        continues to yield new, live events as they are appended to the stream.

        Args:
            from_version: The version from which to start watching for events (inclusive).
                Defaults to the current stream version (watches for new events only).
                To replay all events from the beginning, pass `from_version=0`.
                For the special '@all' stream, this refers to the global sequence ID.

        Yields:
            `StoredEvent` objects from the stream in order.
        """
        ...

    async def metrics(self) -> Dict[str, Any]:
        """
        Returns a dictionary of key metrics for the stream.

        Returns:
            A dictionary containing metrics such as `current_version`, `event_count`,
            and `last_timestamp`.
        """
        ...
