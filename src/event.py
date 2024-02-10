from uuid import UUID
from typing import Optional
from datetime import datetime
from dataclasses import dataclass
from notification import Notification

@dataclass(frozen=True)
class EventAdd:
    notification: Notification

@dataclass(frozen=True)
class EventUpdate:
    id: UUID
    notification_sender: Optional[str] = None
    message_title: Optional[str] = None
    message_body: Optional[str] = None
    schedule_expression: Optional[str]  = None
    date: Optional[datetime] = None
    expiration_date_str: Optional[str] = None
    expiration_date: Optional[datetime] = datetime.fromisoformat(expiration_date_str) if expiration_date_str else None

    
@dataclass(frozen=True)
class EventSend:
    notification: Notification
    
    

@dataclass(frozen=True)
class EventDelete:
    notification_id: UUID

EventType = EventAdd | EventUpdate | EventDelete

SendableEventType = EventSend | EventDelete

Event = Notification | EventType


