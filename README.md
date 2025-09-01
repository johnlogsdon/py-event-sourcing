# Event Sourcing V2

<!--
[PROMPT_SUGGESTION]The `write` method in `streams.py` seems to have a lot of logic. Can you refactor it for clarity?[/PROMPT_SUGGESTION]
[PROMPT_SUGGESTION]Can you add docstrings to the public methods in `streams.py` to explain what they do, their parameters, and what they return?[/PROMPT_SUGGESTION]
-->
A minimal, elegant, and functional event sourcing library for Python, built with `asyncio` and a file-based SQLite backend.

This library provides the core components needed to build event-sourced systems, focusing on simplicity, a clean API, and efficiency. It treats event streams as the single source of truth (SSOT) and provides familiar async I/O operations (`write`, `read`, `watch`) to interact with them.

## Key Features

*   **Simple & Serverless**: Uses SQLite for a file-based, zero-dependency persistence layer. Defaults to a non-persistent, in-memory database if no file is specified.
*   **Idempotent Writes**: Prevents duplicate events in distributed systems by using an optional `id` on each event.
*   **Optimistic Concurrency Control**: Ensures data integrity by allowing writes only against a specific, expected stream version.
*   **Efficient Watching**: A centralized notifier polls the database once to serve all watchers, avoiding the "thundering herd" problem and ensuring low-latency updates.
*   **Snapshot Support**: Accelerate state reconstruction for long-lived streams by saving and loading state snapshots.
*   **Fully Async API**: Built from the ground up with `asyncio` for high-performance, non-blocking I/O.
*   **Extensible by Design**: Core logic is decoupled from storage implementation via `Protocol`-based adapters. While this package provides a highly-optimized SQLite backend, you can easily create your own adapters for other databases (e.g., PostgreSQL, Firestore).

## Installation

This project uses `uv` for dependency management.

1.  **Create and activate the virtual environment:**
    ```bash
    uv venv
    source .venv/bin/activate
    ```

2.  **Install the package in editable mode with dev dependencies:**
    ```bash
    uv pip install -e ".[dev]"
    ```

## Quick Start

Here’s a quick example of writing to and reading from a stream.

```python
import asyncio
from datetime import datetime
from event_sourcing_v2 import sqlite_stream_factory, CandidateEvent

async def main():
    # To use a persistent file-based database:
    # db_path = "my_events.db"
    # To use a non-persistent, in-memory database:
    db_path = ":memory:"

    # The factory is an async context manager that handles all resources.
    async with sqlite_stream_factory(db_path) as open_stream:
        stream_id = "my_first_stream"

        # Write an event
        async with open_stream(stream_id) as stream:
            # Use CandidateEvent to create an event to be written.
            # The timestamp and version are added by the system.
            event = CandidateEvent(type="UserRegistered", data=b'{"user": "Alice"}')
            await stream.write([event])
            print(f"Event written. Stream version is now {stream.version}.")

        # Read the event back
        async with open_stream(stream_id) as stream:
            # Events read from the stream are StoredEvent objects, 
            # which include system-set fields like version and timestamp.
            all_events = [e async for e in stream.read()]
            print(f"Read {len(all_events)} event(s) from the stream.")
            print(f"  -> Event type: {all_events[0].type}, Data: {all_events[0].data.decode()}, Version: {all_events[0].version}")

if __name__ == "__main__":
    asyncio.run(main())
```

## Full Examples

For more detailed examples covering snapshots, watching for live events, and state reconstruction, please see the fully commented example file: `basic_usage.py`.

## Testing

Run the test suite:
```