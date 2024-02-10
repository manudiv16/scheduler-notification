from uuid import UUID
from typing import Any, Dict
from returns.io import IOResult
from dataclasses import dataclass
from abc import ABC, abstractmethod
from notification import Notification
from notification import json_to_notification
from returns.result import Success, Failure, Result
from event import EventType, EventAdd, EventUpdate, EventDelete

@dataclass
class Handler[T](ABC):
    @abstractmethod
    async def handle(self, object: Result[T,Any]) -> IOResult[Any, Any]: 
        pass

def message_to_eventtype(message: Dict[str, Any])  -> Result[EventType, Any]:
        match message:
            case {'command': 'add', 'notification': notification}:
                return json_to_notification(notification).map(lambda x: EventAdd(notification=x))
            case {'command': 'update', 'id': id, **rest }:
                return Success(EventUpdate(id=UUID(id), **rest))
            case {'command': 'delete', 'id': notification_id}:
                return Success(EventDelete(notification_id=UUID(notification_id)))
            case {'command': 'remove', 'id': notification_id}:
                return Success(EventDelete(notification_id=UUID(notification_id)))
            case _:
                return Failure("Fail to parse message to event type")

def message_to_notification(message: Dict[str, Any])  -> Result[Notification, Any]:
        return json_to_notification(message)
        
        

