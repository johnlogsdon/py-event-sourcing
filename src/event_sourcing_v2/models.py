"""
This module defines the core data models for the event sourcing system using Pydantic.
These models serve as the data transfer objects (DTOs) and ensure that all
event and snapshot data is well-structured and validated.
"""
from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any

class Event(BaseModel):
    id: str | None = None  # Optional idempotency key
    type: str
    data: bytes  # Serialized event data
    timestamp: datetime
    metadata: Dict[str, Any] | None = None  # Optional, for other contextual data
    version: int = 0 # Add version to Event model

class Snapshot(BaseModel):
    stream_id: str
    version: int
    state: bytes # Serialized state
    timestamp: datetime
