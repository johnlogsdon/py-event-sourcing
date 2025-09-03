"""
This module exports the main factory function for creating event streams.
"""
from .models import (
    CandidateEvent,
    StoredEvent,
    Snapshot,
    EventFilter,
    EqualsClause,
    InClause,
    LikeClause,
)
from .adaptors.sqlite import sqlite_stream_factory

__all__ = ["CandidateEvent", "StoredEvent", "Snapshot", "sqlite_stream_factory", "EventFilter", "EqualsClause", "InClause", "LikeClause"]
