from typing import Any, AsyncIterable, Callable, Dict, List, Set
import asyncio
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
        self.fernet = Fernet(config["key"])

        # Decrypt events once on load for in-memory operations.
        # This provides a fast, decrypted cache for reads and searches.
        self._events: List[Event] = []
        for event in self.handle.events:  # handle.events has encrypted data
            try:
                decrypted_data = self.fernet.decrypt(event.data)
                self._events.append(event.model_copy(update={"data": decrypted_data}))
            except InvalidToken:
                # Log or handle events that can't be decrypted, e.g., due to key rotation.
                pass

        # Free memory by clearing the now-redundant encrypted event list from the handle.
        self.handle.events.clear()

        self.version = len(self._events)

    async def append(self, events: List[Event], expected_version: int = -1) -> int:
        """
        Appends a list of events to the stream.
        This operation is atomic and idempotent based on event metadata['id'].
        """
        if not all(isinstance(e, Event) for e in events):
            raise TypeError("All items in events list must be Event objects")

        if expected_version != -1 and self.version != expected_version:
            raise ValueError(
                f"Concurrency conflict: expected version {expected_version}, but stream is at {self.version}"
            )

        # Scalable Idempotency Check: Instead of using an in-memory set of all
        # historical event IDs, we query the database only for the IDs in the
        # current batch. This is crucial for performance with large streams.
        idempotency_keys_to_check = [
            event.metadata.get("id")
            for event in events
            if event.metadata and event.metadata.get("id")
        ]
        existing_ids = await self.handle.find_existing_ids(idempotency_keys_to_check)

        new_events_to_persist = []
        new_decrypted_events = []
        for event in events:
            event_id = event.metadata.get("id")
            if not event_id or event_id not in existing_ids:
                encrypted_data = self.fernet.encrypt(event.data)
                new_events_to_persist.append(
                    event.model_copy(update={"data": encrypted_data})
                )
                new_decrypted_events.append(event)

        if not new_events_to_persist:
            return self.version

        await self.handle.sync(new_events_to_persist)

        # Update in-memory state
        for event in new_decrypted_events:
            self._events.append(event)

        self.version += len(new_decrypted_events)
        self.handle.version = self.version  # Keep handle in sync for watch adapter

        return self.version

    async def read_and_watch(
        self, handler: Callable[[Any, Event], Any], initial_state: Any = None
    ) -> AsyncIterable[Any]:
        """
        Processes historical events and then watches for new ones,
        applying a handler to compute a state.
        """
        state = initial_state
        for event in self._events:
            state = await handler(state, event)
            yield state

        # Subscribe to the central notifier instead of polling.
        queue = await self.notifier.subscribe(self.stream_id)
        try:
            while True:
                event = await queue.get()
                try:
                    decrypted_event = event.model_copy(
                        update={"data": self.fernet.decrypt(event.data)}
                    )
                    self._events.append(decrypted_event)
                    self.version += 1
                    self.handle.version = self.version
                    state = await handler(state, decrypted_event)
                    yield state
                except InvalidToken:
                    continue
        finally:
            await self.notifier.unsubscribe(self.stream_id, queue)

    async def search(self, predicate: Callable[[Event], bool]) -> List[Event]:
        return [event for event in self._events if predicate(event)]

    async def get_metrics(self) -> Dict[str, Any]:
        last_ts = self._events[-1].timestamp if self._events else None
        return {
            "current_version": self.version,
            "event_count": self.version,
            "last_timestamp": last_ts,
        }
