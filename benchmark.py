import asyncio
import os
import tempfile
import json
import time
from datetime import datetime, timezone
import functools

from event_sourcing_v2 import Event
from event_sourcing_v2.adaptors.sqlite import sqlite_stream_factory


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


async def write_batch(stream, lock, batch_size):
    events = [Event(type="MyEvent", data=b"data") for _ in range(batch_size)]
    async with lock:
        await stream.write(events)


async def run_benchmark(num_events, batch_size):
    config = {"db_path": ":memory:"}
    async with sqlite_stream_factory(config) as open_stream:
        async with open_stream("test_stream") as stream:
            lock = asyncio.Lock()
            tasks = [
                write_batch(stream, lock, batch_size)
                for _ in range(num_events // batch_size)
            ]
            start_time = time.time()
            await asyncio.gather(*tasks)
            end_time = time.time()

            duration = end_time - start_time
            print(f"Total time for {num_events} events: {duration:.2f} seconds")
            print(f"Events per second: {num_events / duration:.2f}")


if __name__ == "__main__":
    asyncio.run(run_benchmark(num_events=10_000, batch_size=100))
