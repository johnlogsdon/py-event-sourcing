import asyncio
from typing import Any, AsyncIterable, Callable, Dict, List, Set
from cryptography.fernet import Fernet, InvalidToken

from .models import Event
from .notifier import Notifier


class Stream:
    """
    Manages operations on an event stream, including appending, reading,
    and searching events. This class is internal and created by the
    `open_stream` factory.
    """

    def __init__(self, config: Dict, stream_id: str, handle: Any, notifier: Notifier):
        self.config = config
        self.stream_id = stream_id
        self.handle = handle
        self.notifier = notifier
        self.fernet = Fernet(config['key'])
        # The stream is now "lazy" and doesn't load events into memory on startup.
        # The version is fetched from the handle, which gets it from a COUNT query.
        # This is much more memory-efficient for large streams.
        self.version = self.handle.version

    def _decrypt_event(self, event: Event) -> Event:
        """Helper to decrypt a single event's data."""
        decrypted_data = self.fernet.decrypt(event.data)
        return event.model_copy(update={"data": decrypted_data})

    async def append(self, events: List[Event], expected_version: int = -1) -> int:
        """
        Appends a list of events to the stream.
        This operation is atomic and idempotent based on event metadata['id'].
        """
        if not all(isinstance(e, Event) for e in events):
            raise TypeError("All items in events list must be Event objects")

        # The version check is now delegated to the atomic `sync` operation.
        # If the user doesn't provide an expected_version, we use the stream's
        # current known version. This is the optimistic part: we assume our
        # view is up-to-date.
        effective_expected_version = self.version if expected_version == -1 else expected_version

        # Scalable Idempotency Check: Instead of using an in-memory set of all
        # historical event IDs, we query the database only for the IDs in the
        # current batch. This is crucial for performance with large streams.
        idempotency_keys_to_check = [
            event.metadata.get("id") for event in events if event.metadata and event.metadata.get("id")
        ]
        existing_ids = await self.handle.find_existing_ids(idempotency_keys_to_check)

        new_events_to_persist = []
        for event in events:
            event_id = event.metadata.get("id")
            if not event_id or event_id not in existing_ids:
                encrypted_data = self.fernet.encrypt(event.data)
                new_events_to_persist.append(event.model_copy(update={'data': encrypted_data}))

        # The sync method will perform the atomic version check and append.
        await self.handle.sync(new_events_to_persist, effective_expected_version)

        # After a successful append, update our local version.
        self.version += len(new_events_to_persist)
        self.handle.version = self.version  # Keep handle in sync for watch adapter

        return self.version

    async def read_and_watch(
        self, handler: Callable[[Any, Event], Any], initial_state: Any = None
    ) -> AsyncIterable[Any]:
        """
        Processes historical events and then watches for new ones,
        applying a handler to compute a state.
        """
        # Replay historical events by querying the database directly.
        state = initial_state
        async for event in self.handle.get_events():
            try:
                decrypted_event = self._decrypt_event(event)
                state = await handler(state, decrypted_event)
                yield state
            except InvalidToken:
                continue

        # Subscribe to the central notifier instead of polling.
        queue = await self.notifier.subscribe(self.stream_id)
        try:
            while True:
                event = await queue.get()
                try:
                    decrypted_event = self._decrypt_event(event)
                    state = await handler(state, decrypted_event)
                    yield state
                except InvalidToken:
                    continue
        finally:
            await self.notifier.unsubscribe(self.stream_id, queue)

    async def search(self, predicate: Callable[[Event], bool]) -> List[Event]:
        # Search now queries the database directly instead of an in-memory list.
        results = []
        async for event in self.handle.get_events():
            try:
                decrypted_event = self._decrypt_event(event)
                if predicate(decrypted_event):
                    results.append(decrypted_event)
            except InvalidToken:
                continue
        return results

    async def get_metrics(self) -> Dict[str, Any]:
        # This would need to be a query now. For now, we accept it's less accurate.
        last_ts = None
        return {
            "current_version": self.version,
            "event_count": self.version,
            "last_timestamp": last_ts,
        }
