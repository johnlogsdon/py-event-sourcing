# Event Sourcing Concepts

This document explains the core concepts behind the Event Sourcing architectural pattern, including the crucial role of Read Models and Projectors.

## 1. What is Event Sourcing?

Event Sourcing is an architectural pattern where all changes to application state are stored as a sequence of immutable events. Instead of storing just the current state of an entity (like a row in a traditional database), you store every action that has ever occurred for that entity.

**Key Principles:**
*   **Events as the Source of Truth:** The sequence of events is the single, primary record of everything that has happened in the system.
*   **Immutability:** Once an event is recorded, it cannot be changed or deleted. To correct a mistake, you must append a new, compensating event.
*   **State Reconstruction:** The current state of an entity can be derived at any time by replaying all events related to that entity from the beginning (or from a pre-calculated snapshot).

**Benefits:**
*   **Full Audit Trail:** Every change is recorded, providing a complete and reliable history for auditing, debugging, and compliance.
*   **Temporal Queries:** You gain the ability to query the state of the system at any point in the past, which is difficult or impossible in traditional state-based systems.
*   **Decoupling:** Events can be published and consumed by multiple, independent services, enabling highly decoupled and scalable microservice architectures.
*   **Powerful Debugging:** It's easier to understand *how* a system reached a certain state, and you can replay event sequences to reproduce and diagnose bugs.

## 2. Read Models and Projectors

While event sourcing stores the *history* of changes in a write-optimized log, most applications need to query the *current state* efficiently. This is where **Read Models** and **Projectors** come into play.

*   **Read Model (or Query Model):** This is a denormalized, optimized representation of data specifically designed for querying. Unlike the event store, which is write-optimized, read models are read-optimized. They can be stored in any suitable database (e.g., PostgreSQL, Elasticsearch, Redis) depending on the query patterns required.

*   **Projector (or Event Handler/Subscriber):** A projector is a component that listens to the stream of events and updates one or more read models. When a new event occurs, the projector processes it, transforms the data into the format required by the read model, and persists the change.

**How they work together:**
1.  An action occurs in the system, and a new event is appended to an event stream.
2.  Projectors, which are subscribed to the event stream, receive the new event.
3.  The projector applies its logic to update its corresponding read model. For example, a `UserRegistered` event might cause a projector to add a new row to a `users` table in a PostgreSQL database.
4.  User interfaces, APIs, or other services query these read models for current state information, without needing to know about the underlying event store.

This separation of writes (appending events) from reads (querying read models) is a key aspect of the CQRS (Command Query Responsibility Segregation) pattern, which works very well with Event Sourcing.