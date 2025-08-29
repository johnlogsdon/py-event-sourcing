import argparse
import asyncio
import time
from datetime import datetime
from cryptography.fernet import Fernet
from event_sourcing_v2 import open_stream, Event, factories
import os
import tempfile

async def benchmark(num_events: int):
    print(f"Benchmarking with {num_events} events...")

    async def run_mode(mode: str, config: dict, num_events: int):
        # The stream needs to be opened for each run to have a clean state
        async with open_stream(config, "bench_stream") as stream:
            # --- Append benchmark ---
            # Create all events at once to send in a single batch.
            # This avoids the performance penalty of repeated small appends if the
            # underlying append logic has non-linear complexity.
            all_new_events = [
                Event(type="Bench", data=f"data{i}".encode(), timestamp=datetime.now(), metadata={})
                for i in range(num_events)
            ]
            start_append = time.perf_counter()
            await stream.append(all_new_events)
            append_time = time.perf_counter() - start_append

            # --- Read benchmark ---
            start_read = time.perf_counter()
            all_events = await stream.search(lambda e: True)
            read_time = time.perf_counter() - start_read

            assert len(all_events) == num_events

        return append_time, read_time

    # In-memory SQLite mode
    key = Fernet.generate_key()
    memory_config = {"key": key, "url": "sqlite://"}
    mem_append_time, mem_read_time = await run_mode("In-memory SQLite", memory_config, num_events)
    mem_append_throughput = num_events / mem_append_time if mem_append_time > 0 else 0
    mem_read_throughput = num_events / mem_read_time if mem_read_time > 0 else 0

    # File-based SQLite mode
    key = Fernet.generate_key()
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = os.path.join(tmpdir, "bench.db")
        file_config = {"key": key, "url": f"sqlite:///{db_path}"}
        file_append_time, file_read_time = await run_mode("File-based SQLite", file_config, num_events)
        file_append_throughput = num_events / file_append_time if file_append_time > 0 else 0
        file_read_throughput = num_events / file_read_time if file_read_time > 0 else 0

    print(f"\n--- Results for {num_events} events ---")
    print(f"In-memory SQLite  - Append: {mem_append_time:.4f}s ({mem_append_throughput:,.0f} events/s), Read: {mem_read_time:.4f}s ({mem_read_throughput:,.0f} events/s)")
    print(f"File-based SQLite - Append: {file_append_time:.4f}s ({file_append_throughput:,.0f} events/s), Read: {file_read_time:.4f}s ({file_read_throughput:,.0f} events/s)")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-events", type=int, default=1000)
    args = parser.parse_args()
    await benchmark(args.num_events)
    # Explicitly clean up all notifier background tasks and shared connections to allow the script to exit.
    await factories.cleanup_shared_resources()

if __name__ == "__main__":
    asyncio.run(main())
