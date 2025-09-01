# Event Sourcing Concepts and Project Features

This document explains the core concepts behind Event Sourcing and highlights how this `event-sourcing-v2` library implements them, along with the crucial role of Read Models and Projectors.

## 1. What is Event Sourcing?

Event Sourcing is an architectural pattern where all changes to application state are stored as a sequence of immutable events. Instead of storing the current state of an entity (like a row in a traditional database), you store every action that led to that state.

**Key Principles:**
*   **Events as the Source of Truth:** The sequence of events is the primary record of everything that has happened in the system.
*   **Immutability:** Once an event is recorded, it cannot be changed or deleted.
*   **State Reconstruction:** The current state of an entity can be derived by replaying all events related to that entity from the beginning of time (or from a snapshot).

**Benefits:**
*   **Full Audit Trail:** Every change is recorded, providing a complete history for auditing, debugging, and compliance.
*   **Temporal Queries:** Ability to query the state of the system at any point in the past.
*   **Decoupling:** Events can be consumed by multiple services or components, enabling highly decoupled and scalable architectures.
*   **Debugging & Replay:** Easier to understand "how" a state was reached and to replay scenarios for debugging or testing.

## 2. Key Features of `event-sourcing-v2`

This library is designed to be a minimal, elegant, and functional event sourcing library for Python, built with `asyncio` and a file-based SQLite backend. It provides the core components needed to build event-sourced systems, focusing on simplicity, a clean API, and efficiency.

*   **Simple & Serverless**: Uses SQLite for a file-based, zero-dependency persistence layer. Defaults to a non-persistent, in-memory database if no file is specified.
*   **Idempotent Writes**: Prevents duplicate events in distributed systems by using an optional `id` on each event.
*   **Optimistic Concurrency Control**: Ensures data integrity by allowing writes only against a specific, expected stream version.
*   **Efficient Watching**: A centralized notifier polls the database once to serve all watchers, avoiding the "thundering herd" problem and ensuring low-latency updates.
*   **Snapshot Support**: Accelerate state reconstruction for long-lived streams by saving and loading state snapshots.
*   **Fully Async API**: Built from the ground up with `asyncio` for high-performance, non-blocking I/O.
*   **Extensible by Design**: Core logic is decoupled from the implementation via `Protocol`-based adapters, allowing for future extensions.
*   **Simple Factory Entry Point**: The primary public API is the `sqlite_stream_factory`, a Higher-Order Function that provides a simple async context manager for all database and stream resources, ensuring they are correctly initialized and shut down.

## 3. Technical Deep Dive: High-Performance SQLite Backend

While this library is designed to be extensible, the built-in SQLite adapter is highly optimized for performance and concurrency. The `sqlite_stream_factory` is the recommended entry point and encapsulates the following best practices:

*   **Write-Ahead Logging (WAL Mode)**: The database is configured in WAL mode (`PRAGMA journal_mode=WAL;`). This is a crucial setting that allows read operations to occur concurrently with write operations, significantly improving throughput in multi-threaded or `asyncio`-based applications.

*   **Advanced Connection Management**: To prevent deadlocks and contention, the factory manages three separate connection resources:
    1.  **A Single Write Connection**: All write operations are serialized through a single, dedicated database connection, ensuring consistency and preventing `database is locked` errors.
    2.  **A Pool of Read Connections**: Read operations are distributed across a pool of read-only connections, allowing multiple concurrent readers without blocking the writer.
    3.  **A Dedicated Notifier Connection**: The `Notifier` uses its own connection to poll for changes, isolating it from application reads and writes and ensuring low-latency event notifications.

*   **Performance Tuning**: The connections are tuned for high performance in a server environment:
    *   `PRAGMA synchronous = NORMAL`: In WAL mode, this setting provides a significant write performance boost with a very low risk of corruption, as it avoids waiting for the OS to confirm that data is physically written to the disk.
    *   `PRAGMA cache_size`: The in-memory page cache is increased to provide more memory for caching frequently accessed data, reducing disk I/O for read operations.

## 4. Read Models and Projectors

While event sourcing stores the *history* of changes, most applications need to query the *current state* efficiently. This is where **Read Models** and **Projectors** come into play.

*   **Read Model (or Query Model):** This is a denormalized, optimized representation of data specifically designed for querying. Unlike the event store, which is write-optimized, read models are read-optimized. They can be stored in any suitable database (e.g., relational, NoSQL, search index) depending on the query patterns required. This library's built-in snapshot support can be considered a specialized form of read model, allowing for rapid state reconstruction without replaying all historical events.

*   **Projector (or Event Handler/Subscriber):** A projector is a component that listens to events from the event store and updates one or more read models. When a new event occurs, the projector processes it and transforms the event data into a format suitable for the read model, then persists it.

**How they work together:**
1.  An action occurs in the system, and a new event is appended to an event stream in the `event-sourcing-v2` library.
2.  Projectors (separate services or functions) subscribe to these event streams (e.g., using the `stream.watch()` feature).
3.  When a projector receives an event, it applies the logic to update its corresponding read model.
4.  User interfaces or other services query these read models for current state information, without needing to replay events or understand the event-sourced history.

**Example with `event-sourcing-v2`:**

In `event-sourcing-v2`, you would typically implement a projector by:
1.  Using `stream.watch()` to continuously receive new events for a given stream or across multiple streams.
2.  Inside your projector logic, you would process each event and update a separate database (e.g., a PostgreSQL database, a Redis cache, or even a simple in-memory dictionary for small applications) that serves as your read model.
3.  The `basic_usage.py` example's `CounterState` and its `apply` method demonstrate a simple form of state reconstruction, which is the core logic a projector would use to build a read model. For a persistent read model, you would replace the in-memory `CounterState` with writes to a separate database.