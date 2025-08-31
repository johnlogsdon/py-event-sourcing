"""
This module defines the abstract protocols for storage and notification.

By using `Protocol`-based interfaces, the core stream logic is decoupled from
the concrete implementation details of the backend. This makes the system
extensible, allowing for future support of different databases (e.g., PostgreSQL)
or notification systems (e.g., Redis Pub/Sub) without changing the `Stream` class.
This is a key principle of the library's design.
"""
from typing import Protocol, AsyncIterable, List, Set, Dict, Any
import asyncio
from datetime import datetime

from .models import Event, Snapshot # Import Snapshot

class StorageHandle(Protocol):
    """
    Defines the contract that all storage adapters must implement.
    The Stream class interacts with this protocol, not a concrete implementation.
    """
    version: int

    async def sync(self, new_events: List[Event], expected_version: int):
        ...

    async def find_existing_ids(self, event_ids: List[str]) -> Set[str]:
        ...

    async def get_events(self, start_version: int = 0) -> AsyncIterable[Event]:
        ...

    async def get_last_timestamp(self) -> datetime | None:
        ...

    async def save_snapshot(self, snapshot: Snapshot):
        ...

    async def load_latest_snapshot(self, stream_id: str) -> Snapshot | None:
        ...

    async def close(self):
        ...

class Notifier(Protocol):
    """
    Defines the contract for notifying watchers of new events.
    This allows for different notification strategies (e.g., polling, pub/sub).
    """
    async def start(self):
        ...
    async def stop(self):
        ...
    async def subscribe(self, stream_id: str) -> asyncio.Queue:
        ...
    async def unsubscribe(self, stream_id: str, queue: "asyncio.Queue"):
        ...

class Stream(Protocol):
    """
    Defines the public interface for an event stream.
    This allows the factory to return a stream object without coupling the user
    to a specific implementation class.
    """
    version: int
    stream_id: str

    async def write(self, events: List[Event], expected_version: int = -1) -> int:
        ...

    async def snapshot(self, state: bytes):
        ...

    async def load_snapshot(self) -> Snapshot | None:
        ...

    async def read(self, from_version: int = 0) -> AsyncIterable[Event]:
        ...

    async def watch(self, from_version: int | None = None) -> AsyncIterable[Event]:
        ...

    async def metrics(self) -> Dict[str, Any]:
        ...