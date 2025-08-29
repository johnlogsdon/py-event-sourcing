from pydantic import BaseModel
from datetime import datetime
from typing import Dict, Any

class Event(BaseModel):
    type: str
    data: bytes  # Encrypted payload
    timestamp: datetime
    metadata: Dict[str, Any]  # Unencrypted for filtering/search
