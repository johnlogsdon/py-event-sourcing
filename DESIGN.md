# Design Document: Event Sourcing V2

This document provides a high-level overview of the architecture, components, and design philosophy of the `event-sourcing-v2` library.

## 1. Philosophy

The library is built on a few core principles:

*   **Minimal & Functional**: Provide a small, elegant API that focuses on the core tenets of event sourcing without including non-essential features like a built-in gRPC server or workflow manager.
*   **Async First**: The entire library is built on `asyncio` to ensure high-performance, non-blocking I/O suitable for modern applications.
*   **Extensible by Design**: The core logic is decoupled from the storage implementation. This is achieved by defining clear contracts (`Protocols`) for storage and notification, allowing users to easily swap out the default SQLite backend for any other database.
*   **Robust & Performant**: The default SQLite implementation is heavily optimized for high-concurrency scenarios, using WAL mode, dedicated read/write connections, and sensible `PRAGMA` settings to ensure both safety and speed.

## 2. Architecture Overview

The library follows a layered architecture with a clear separation of concerns between the public API, the core stream logic, the storage protocols, and the concrete storage implementation.

```mermaid
graph TD
    subgraph User Code
        A[User Application]
    end

    subgraph "event_sourcing_v2 (Public API)"
        B(sqlite_stream_factory)
    end

    subgraph "Internal Components"
        C[StreamImpl]
        D[SQLiteStorageHandle]
        E[SQLiteNotifier]
    end

    subgraph "Protocols (Interfaces)"
        P1(Stream)
        P2(StorageHandle)
        P3(Notifier)
    end

    subgraph Database
        DB[(SQLite DB)]
    end

    A -- "Calls factory with db_path" --> B
    B -- "Creates & injects dependencies" --> C
    B -- "Creates" --> D
    B -- "Creates" --> E
    C -- "Implements" --> P1
    D -- "Implements" --> P2
    E -- "Implements" --> P3
    C -- "Uses (write, read)" --> D
    C -- "Uses (watch)" --> E
    D -- "Interacts with" --> DB
    E -- "Polls" --> DB
```

### Flow Description

1.  **Initialization**: The user's application calls the `sqlite_stream_factory` with a database path. This factory is an `asynccontextmanager` that manages the entire lifecycle of the required resources.
2.  **Dependency Creation**: The factory creates instances of the `SQLiteStorageHandle` (for reads/writes) and the `SQLiteNotifier` (for watching).
3.  **Stream Instantiation**: For each stream the user requests, the factory injects the storage handle and notifier into a `StreamImpl` instance. The user interacts with this `StreamImpl` instance through the `Stream` protocol, ensuring they are not coupled to the implementation details.
4.  **Operations**: When the user calls methods like `write()`, `read()`, or `watch()` on the stream, `StreamImpl` delegates these calls to the appropriate backend component (`SQLiteStorageHandle` or `SQLiteNotifier`), which then interacts with the SQLite database.

## 3. Component Deep Dive

### `sqlite_stream_factory`

*   **Role**: The single public entry point for creating and managing SQLite-backed event streams.
*   **Responsibility**:
    *   Manages the lifecycle of all database connections (a dedicated write connection, a pool of read connections, and a notifier connection).
    *   Initializes the database schema if it doesn't exist.
    *   Instantiates and wires together the `SQLiteStorageHandle`, `SQLiteNotifier`, and `StreamImpl` components.
    *   Provides a simple, clean factory function to the user, abstracting away all the underlying complexity.

### `StreamImpl`

*   **Role**: The core implementation of the `Stream` protocol. It orchestrates the business logic of an event stream.
*   **Responsibility**:
    *   Maintains the in-memory state of a stream (e.g., `stream_id`, `version`).
    *   Handles optimistic concurrency checks.
    *   Delegates persistence and retrieval operations to the `StorageHandle` injected into it.
    *   Delegates the `watch` operation to the `Notifier` injected into it.
    *   Is completely agnostic of the underlying storage mechanism.

### `SQLiteStorageHandle`

*   **Role**: The concrete implementation of the `StorageHandle` protocol for SQLite.
*   **Responsibility**:
    *   Executes all low-level database operations (INSERT, SELECT) for events and snapshots.
    *   Manages transactions (`SAVEPOINT`) to ensure atomic writes.
    *   Implements idempotency checks to prevent duplicate events.
    *   Contains all SQL queries and logic specific to the SQLite database schema.

### `SQLiteNotifier`

*   **Role**: The concrete implementation of the `Notifier` protocol for SQLite.
*   **Responsibility**:
    *   Runs a background task that periodically polls the `events` table for new records.
    *   Manages a list of active watchers (subscribers).
    *   Notifies all relevant watchers when new events are detected for their stream.
    *   Handles the "replay" of historical events when a watcher connects.

## 4. Extensibility Guide

The library is designed to be extended with different storage backends. To create a new adapter (e.g., for PostgreSQL), you would need to do the following:

1.  **Implement `StorageHandle`**: Create a class that implements the `StorageHandle` protocol defined in `protocols.py`. This class would contain all the database-specific logic for writing, reading, and managing snapshots for your chosen database.
2.  **Implement `Notifier`**: Create a class that implements the `Notifier` protocol. The implementation will depend on the capabilities of your database. For PostgreSQL, you could use the `LISTEN`/`NOTIFY` feature for a highly efficient, push-based implementation instead of polling.
3.  **Create a Factory Function**: Write your own factory function (e.g., `postgres_stream_factory`) that mirrors the role of `sqlite_stream_factory`. It would be responsible for managing connections to your database and injecting your new `PostgresStorageHandle` and `PostgresNotifier` into the `StreamImpl`.

By adhering to these protocols, your new storage adapter will seamlessly integrate with the core `StreamImpl` logic without requiring any changes to it.
