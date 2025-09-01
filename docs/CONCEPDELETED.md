# Event Sourcing Concepts

This document explains the core concepts behind the Event Sourcing architectural pattern, including the crucial role of Read Models and Projectors.

## 1. What is Event Sourcing?

Event Sourcing stores all changes to application state as a sequence of immutable events. Instead of storing the current state, you store the full history of actions taken on an entity.

**Key Principles:**
*   **Events as Source of Truth**: The event log is the primary record of everything that has happened.
*   **Immutability**: Events cannot be changed or deleted. Corrections are made by appending a compensating event.
*   **State Reconstruction**: Current state is derived by replaying events from the beginning or from a snapshot.

**Benefits:**
*   **Full Audit Trail**: A complete history for auditing, debugging, and compliance.
*   **Temporal Queries**: Query the state of the system at any point in the past.
*   **Decoupling**: Events can be consumed by multiple, independent services, enabling scalable architectures.
*   **Powerful Debugging**: Replay events to reproduce and diagnose bugs.

## 2. Read Models and Projectors

The event store is optimized for writes. For efficient queries, applications use **Read Models** and **Projectors**.

*   **Read Model**: A denormalized, read-optimized representation of data for querying. It can be stored in any suitable database (e.g., PostgreSQL, Elasticsearch, Redis).

*   **Projector**: A component that listens to events and updates read models. It processes new events, transforms the data, and persists the changes to the read model.

**Workflow:**
1.  A new event is appended to an event stream.
2.  A projector subscribed to the stream receives the event.
3.  The projector updates its read model based on the event.
4.  APIs and UI services query the read model for current state.

This separation of reads and writes is a key aspect of the CQRS (Command Query Responsibility Segregation) pattern, which complements Event Sourcing.
