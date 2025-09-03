"""
This module exports the main factory function for creating event streams.
"""

from .adaptors.sqlite import sqlite_stream_factory
from .models import (
    CandidateEvent,
    EqualsClause,
    EventFilter,
    InClause,
    LikeClause,
    Snapshot,
    StoredEvent,
)

__all__ = [
    "CandidateEvent",
    "StoredEvent",
    "Snapshot",
    "sqlite_stream_factory",
    "EventFilter",
    "EqualsClause",
    "InClause",
    "LikeClause",
]
