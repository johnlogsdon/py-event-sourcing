"""
This module defines the core data models for the event sourcing system using Pydantic.
These models serve as the data transfer objects (DTOs) and ensure that all
event and snapshot data is well-structured and validated.
"""
import uuid
from pydantic import BaseModel, Field
from datetime import datetime
from typing import Dict, Any, Optional, List, Literal, Union

class CandidateEvent(BaseModel):
    idempotency_key: Optional[str] = None
    type: str
    data: bytes
    metadata: Optional[Dict[str, Any]] = None

class StoredEvent(BaseModel):
    id: str
    stream_id: str
    sequence_id: int
    version: int
    timestamp: datetime
    type: str
    data: bytes
    metadata: Optional[Dict[str, Any]] = None

class Snapshot(BaseModel):
    stream_id: str
    # The name of the projection this snapshot is for.
    # Allows a single event stream to have snapshots for multiple read models.
    projection_name: str = "default"
    version: int
    state: bytes # Serialized state
    timestamp: datetime


# -----------------------------------------------------------------------------
# Event Filtering Models
# -----------------------------------------------------------------------------

class EqualsClause(BaseModel):
    op: Literal["="] = "="
    field: str
    value: Any

class InClause(BaseModel):
    op: Literal["in"] = "in"
    field: str
    value: List[Any] = Field(..., min_length=1)

class LikeClause(BaseModel):
    op: Literal["like"] = "like"
    field: str
    value: str

FilterClause = Union[EqualsClause, InClause, LikeClause]

class EventFilter(BaseModel):
    clauses: List[FilterClause]
