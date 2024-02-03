import marshmallow_dataclass

from enum import Enum
from uuid import UUID
from datetime import datetime
from croniter import croniter
from dataclasses import dataclass
from typing import Any, Dict, Optional
from returns.result import Failure, Success, Result
from marshmallow import ValidationError, validates_schema

class NotificationStatus(Enum):
    WAITING = 1
    SENDED = 2
    EXPIRED = 3
    SEND = 4

@dataclass(frozen=True)
class Notification:
    id: UUID
    user_id: UUID
    notification_sender: str
    message_title: str
    message_body: str
    schedule_expression: Optional[str]  = None
    next_time: Optional[int] = None
    date: Optional[datetime] = None
    expiration_date_str: Optional[str] = None
    expiration_date: Optional[datetime] = datetime.fromisoformat(expiration_date_str) if expiration_date_str else None

    @validates_schema
    def validate_expiration_date_str(self, data: Dict[str, Any], **kwargs: Dict[Any, Any]) -> None:
        expiration_date_str = data.get('expiration_date_str')
        if expiration_date_str:
            try:
                datetime.fromisoformat(expiration_date_str)
            except ValueError as err:
                raise ValidationError("Invalid expiration_date_str format.", field_names=['expiration_date_str'])

    @validates_schema
    def validate_schedule_or_date(self, data: Dict[str, Any] , **kwargs: Dict[Any, Any]) -> None:
        schedule_expression = data.get('schedule_expression')
        date = data.get('date')

        if schedule_expression and date:
            raise ValidationError("Only one of 'schedule_expression' or 'date' can be provided.", field_names=['schedule_expression', 'date'])
        if schedule_expression is None and date is None:
            raise ValidationError("Either 'schedule_expression' or 'date' must be provided.", field_names=['schedule_expression', 'date'])

def match(schedule_expression: Optional[str], date: Optional[datetime], now:datetime) -> bool:
    if schedule_expression:
        return croniter.match(schedule_expression, now)
    if date:
        return date == now
    return False
    
def expired(expiration_date: Optional[datetime], date: Optional[datetime], now: datetime) -> bool:
    if expiration_date:
        return expiration_date < now
    elif date:
        return date < now
    else:
        return False
    
def if_sent(next_time:  Optional[int], now: datetime) -> bool:
    if next_time:
        timestamp: int = next_time
        next : datetime = datetime.fromtimestamp(timestamp)
        return next > now 
    return False


def get_next_time(expression: str, now: datetime) -> datetime:
    cron = croniter(expression, now)
    return cron.get_next(datetime) # type: ignore

def get_status(notification: Notification, now: datetime) -> Result[NotificationStatus, Any]:
    try:
        if expired(notification.expiration_date, notification.date, now):
            return Success(NotificationStatus.EXPIRED)
        elif match(notification.schedule_expression,notification.date, now):
            if if_sent(notification.next_time, now):
                return Success(NotificationStatus.SENDED)
            else:
                return Success(NotificationStatus.SEND)
        else:
            return Success(NotificationStatus.WAITING)
    except Exception as err:
        return Failure(err)
    
def json_to_notification(json: Any) -> Result[Notification, Any] : 
    try:
        notification_schema = marshmallow_dataclass.class_schema(Notification)()
        notification = notification_schema.load(dict(json))
        return Success(notification)
    except Exception as err:
        return Failure(err)


__all__ = [ "Notification", "NotificationStatus", "get_status", "json_to_notification" ]