from pydantic import BaseModel

class Event(BaseModel):
    user_id: str
    timestamp: int
    event_type: str
    metadata: dict