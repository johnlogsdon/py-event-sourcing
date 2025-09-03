import asyncio
import os
import tempfile
import json

from py_event_sourcing import (
    sqlite_stream_factory,
    CandidateEvent,
    StoredEvent,
    EventFilter,
    EqualsClause,
    LikeClause,
)


# This is a simple state model for our example.
# In a real application, this would likely be a more complex domain object.
class CounterState:
    def __init__(self):
        self.count = 0

    def apply(self, event: StoredEvent):
        """Applies an event to modify the state."""
        if event.type == "Increment":
            self.count += 1
        elif event.type == "Decrement":
            self.count -= 1

    def to_snapshot_data(self) -> bytes:
        """Serializes the state for snapshotting."""
        return json.dumps({"count": self.count}).encode("utf-8")

    @classmethod
    def from_snapshot_data(cls, data: bytes) -> "CounterState":
        """Deserializes the state from a snapshot."""
        state = cls()
        state.count = json.loads(data.decode("utf-8"))["count"]
        return state


async def run_examples():
    """
    This function demonstrates the core features of the event sourcing library.
    """
    # Use a temporary directory for the database to keep the example self-contained.
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "example.db")

        # The stream_factory is an async context manager that handles resource setup and teardown.
        async with sqlite_stream_factory(db_path) as open_stream:
            stream_id = "counter_stream_1"
            print("--- Example 1: Writing and Reading Events ---")
            # Open a stream. The `async with` block handles resource management.
            async with open_stream(stream_id) as stream:
                # Create some events. `id` is used for idempotency.
                events_to_write = [
                    CandidateEvent(type="Increment", data=b"", idempotency_key="event-1"),
                    CandidateEvent(type="Increment", data=b"", idempotency_key="event-2"),
                    CandidateEvent(type="Decrement", data=b"", idempotency_key="event-3"),
                ]
                # Write events to the stream. This is an atomic operation.
                new_version = await stream.write(events_to_write)
                print(f"Stream '{stream_id}' is now at version {new_version}.")

            # Re-open the stream to read the events back.
            async with open_stream(stream_id) as stream:
                print("Reading all events from the stream:")
                all_events = [e async for e in stream.read()]
                for event in all_events:
                    print(f"  - Event version {event.version}: {event.type}")
                assert len(all_events) == 3

            print("\n--- Example 2: Reconstructing State from Events (Read Model & Projector) ---")

            # --- Read Model Definition ---
            # The `CounterState` class acts as our Read Model. It's a simplified
            # representation of the current state derived from events, optimized for queries.
            # In a real application, this might be stored in a separate database (e.g., PostgreSQL, Redis).

            async def reconstruct_state_with_projector(stream_instance, initial_state=None):
                """
                This function acts as a simple Projector.
                It reads events from the stream and applies them to a Read Model
                to reconstruct its current state.
                """
                state = initial_state if initial_state is not None else CounterState()
                async for event in stream_instance.read():
                    state.apply(event)
                return state

            # --- Projector in Action ---
            async with open_stream(stream_id) as stream:
                # Use our projector function to reconstruct the state
                final_state = await reconstruct_state_with_projector(stream)
                print(f"Reconstructed state: Counter is {final_state.count}.")
                assert final_state.count == 1  # (2 increments, 1 decrement)

            print("\n--- Example 3: Watching for New Events ---")
            # The `watch` method replays historical events and then waits for new ones.

            async def consume_stream():
                # The watcher opens its own stream instance.
                async with open_stream(stream_id) as stream:
                    print("Watching for events (historical and new)...")
                    events_seen = 0
                    async for event in stream.watch(from_version=0):
                        print(f"  - Watched event: {event.type} (version {event.version})")
                        events_seen += 1
                        if events_seen == 5:  # We expect 3 historical + 2 new
                            break

            watch_task = asyncio.create_task(consume_stream())
            await asyncio.sleep(0.5)  # Give the watcher time to start up and subscribe

            print("Writing new events to trigger the watcher...")
            # A separate task or part of the application can write to the same stream.
            async with open_stream(stream_id) as writer_stream:
                await writer_stream.write(
                    [
                        CandidateEvent(type="Increment", data=b"", idempotency_key="event-4"),
                        CandidateEvent(type="Increment", data=b"", idempotency_key="event-5"),
                    ]
                )
            await asyncio.wait_for(watch_task, timeout=5)

            print("\n--- Example 4: Using Snapshots for Efficiency ---")
            async with open_stream(stream_id) as stream:
                state = CounterState()
                async for event in stream.read():
                    state.apply(event)
                # When saving, we can specify a projection name.
                # If we have multiple ways of interpreting the event stream,
                # we can save a separate snapshot for each.
                await stream.snapshot(state.to_snapshot_data(), projection_name="counter")
                print(
                    f"Snapshot for 'counter' projection saved at version {stream.version} with state: count = {state.count}"
                )
                await stream.write(
                    [
                        CandidateEvent(type="Increment", data=b"", idempotency_key="event-6")
                    ]
                )

            async with open_stream(stream_id) as stream:
                latest_snapshot = await stream.load_snapshot(projection_name="counter")
                if latest_snapshot:
                    state = CounterState.from_snapshot_data(latest_snapshot.state)
                    print(
                        f"State for 'counter' projection restored from snapshot at version {latest_snapshot.version}. Count is {state.count}."
                    )
                    print("Replaying events since snapshot...")
                    async for event in stream.read(from_version=latest_snapshot.version):
                        print(f"  - Applying event version {event.version}: {event.type}")
                        state.apply(event)
                    print(f"Final reconstructed state: Counter is {state.count}.")
                    assert state.count == 4  # 3 (from snapshot) + 1 (event 6)
            
            print("\n--- Example 5: Filtering Events from the @all Stream ---")
            # The @all stream allows reading events from all streams.
            # We can use filters to select only the events we are interested in.
            async with open_stream("@all") as all_stream:
                # Let's find all "Increment" events from our specific stream
                event_filter = EventFilter(clauses=[
                    EqualsClause(field="stream_id", value=stream_id),
                    EqualsClause(field="event_type", value="Increment"),
                ])
                
                print(f"Reading all 'Increment' events for stream '{stream_id}':")
                filtered_events = [e async for e in all_stream.read(event_filter=event_filter)]
                
                for event in filtered_events:
                    print(f"  - Found event: stream={event.stream_id}, type={event.type}, version={event.version}")
                
                # We wrote 6 increment events in total (5 before snapshot, 1 after)
                assert len(filtered_events) == 5


if __name__ == "__main__":
    asyncio.run(run_examples())
