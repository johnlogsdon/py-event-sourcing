import asyncio
import os
import tempfile
import json
import time
from datetime import datetime, timezone
import functools

from event_sourcing_v2 import Event
from event_sourcing_v2.adaptors.sqlite import SQLiteStorage


class CounterState:
    def __init__(self):
        self.count = 0

    def apply(self, event: Event):
        if event.type == "Increment":
            self.count += 1
        elif event.type == "Decrement":
            self.count -= 1

    def to_snapshot_data(self) -> bytes:
        return json.dumps({"count": self.count}).encode("utf-8")

    @classmethod
    def from_snapshot_data(cls, data: bytes) -> "CounterState":
        state = cls()
        state.count = json.loads(data.decode("utf-8"))["count"]
        return state


async def reconstruct_state_with_projector(stream_instance, initial_state=None, from_version=None):
    state = initial_state if initial_state is not None else CounterState()
    if from_version is not None:
        async for event in stream_instance.read(from_version=from_version):
            state.apply(event)
    else:
        async for event in stream_instance.read():
            state.apply(event)
    return state


async def write_batch(stream, batch, lock):
    async with lock:
        await stream.write(batch)


async def run_benchmark():
    num_events = 1_000_000
    batch_size = 10000
    stream_id = "benchmark_stream"

    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "benchmark.db")
        config = {"db_path": db_path}

        print(f"--- Running benchmark with {num_events} events ---")

        # --- Phase 1: Write all events ---
        print(f"Writing {num_events} events...\n")
        start_time = time.time()

        event_batches = []
        for i in range(0, num_events, batch_size):
            events_to_write = []
            for j in range(i, i + batch_size):
                events_to_write.append(
                    Event(
                        type="Increment",
                        data=b"",
                        timestamp=datetime.now(timezone.utc),
                        id=f"event-{j}",
                    )
                )
            event_batches.append(events_to_write)

        async with SQLiteStorage(config) as storage:
            async with storage.open_stream(stream_id) as stream:
                lock = asyncio.Lock()
                tasks = []
                for batch in event_batches:
                    tasks.append(write_batch(stream, batch, lock))

                await asyncio.gather(*tasks)

            write_time = time.time() - start_time
            print(f"Finished writing {num_events} events in {write_time:.2f} seconds.")

            async with storage.open_stream(stream_id) as stream:

                # --- Benchmark 1: Reconstruct state from all events (no snapshot) ---
                print("\n--- Benchmark 1: Reconstructing state from all events ---")
                start_time = time.time()
                final_state = await reconstruct_state_with_projector(stream)
                reconstruct_all_time = time.time() - start_time
                print(f"Reconstructed state (all events): Counter is {final_state.count}.")
                print(f"Time to reconstruct from all events: {reconstruct_all_time:.2f} seconds.")
                assert final_state.count == num_events

                # --- Benchmark 2: Reconstruct state using snapshot ---
                print("\n--- Benchmark 2: Reconstructing state using snapshot ---")

                # Create a snapshot at the current version (after all 1M events)
                print("Creating snapshot...")
                state_for_snapshot = await reconstruct_state_with_projector(stream)
                await stream.snapshot(state_for_snapshot.to_snapshot_data(), projection_name="counter_snapshot")
                print("Snapshot created.")

                # Write a few more events after the snapshot
                num_additional_events = 100
                print(f"Writing {num_additional_events} additional events...")
                additional_events = []
                for i in range(num_additional_events):
                    additional_events.append(
                        Event(
                            type="Increment",
                            data=b"",
                            timestamp=datetime.now(timezone.utc),
                            id=f"event-{num_events + i}",
                        )
                    )
                await stream.write(additional_events)
                print(f"Finished writing {num_additional_events} additional events.")

                start_time = time.time()
                latest_snapshot = await stream.load_snapshot(projection_name="counter_snapshot")
                if latest_snapshot:
                    state_from_snapshot = CounterState.from_snapshot_data(latest_snapshot.state)
                    print(f"State restored from snapshot at version {latest_snapshot.version}.")
                    final_state_snapshot = await reconstruct_state_with_projector(
                        stream,
                        initial_state=state_from_snapshot,
                        from_version=latest_snapshot.version
                    )
                else:
                    final_state_snapshot = await reconstruct_state_with_projector(stream)

                reconstruct_snapshot_time = time.time() - start_time
                print(f"Reconstructed state (with snapshot): Counter is {final_state_snapshot.count}.")
                print(f"Time to reconstruct with snapshot: {reconstruct_snapshot_time:.2f} seconds.")
                assert final_state_snapshot.count == num_events + num_additional_events

                # --- Final Stream Metrics ---
                print("\n--- Final Stream Metrics ---")
                print("stream metrics:", await stream.metrics())
                await asyncio.sleep(0.1) # Give Notifier a moment

        print("\n--- Benchmark Summary ---")
        print(f"Time to reconstruct from all {num_events + num_additional_events} events: {reconstruct_all_time:.2f} seconds.")
        print(f"Time to reconstruct with snapshot (after {num_events} events): {reconstruct_snapshot_time:.4f} seconds.")

        # Print DB file size before deletion
        db_size_bytes = os.path.getsize(db_path)
        db_size_mb = db_size_bytes / (1024 * 1024)
        print(f"\nDatabase file size: {db_size_mb:.2f} MB")


if __name__ == "__main__":
    asyncio.run(run_benchmark())
