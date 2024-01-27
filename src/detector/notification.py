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
    def validate_schedule_or_date(self, data, **kwargs):
        schedule_expression = data.get('schedule_expression')
        date = data.get('date')

        if schedule_expression and date:
            raise ValidationError("Only one of 'schedule_expression' or 'date' can be provided.", field_names=['schedule_expression', 'date'])
        if schedule_expression is None and date is None:
            raise ValidationError("Either 'schedule_expression' or 'date' must be provided.", field_names=['schedule_expression', 'date'])
            
def json_to_notification(json: Dict[str, Any]) -> Result[Notification, ValidationError] : 
    try:
        notification_schema = marshmallow_dataclass.class_schema(Notification)()
        return Success(notification_schema.load(dict(json)))
    except ValidationError as err:
        return Failure(err)


def match(notification: Notification, now: datetime) -> bool:
    schedule_expression = notification.schedule_expression
    if schedule_expression:
        return croniter.match(schedule_expression, now)
    if notification.date:
        date: datetime = notification.date
        return date == now
    return False

def expired(notification: Notification, now: datetime) -> bool:
    expiration_date: Optional[datetime] = notification.expiration_date
    date: Optional[datetime]  = notification.date

    if expiration_date:
        return expiration_date < now
    elif date:
        return date < now
    else:
        return False

def if_sent(notification: Notification, now: datetime) -> bool:
    if notification.next_time:
        timestamp: int = notification.next_time
        next_time: datetime = datetime.fromtimestamp(timestamp)
        return next_time > now 
    return False


def get_next_time(expression: str, now: datetime) -> datetime:
    cron = croniter(expression, now)
    return cron.get_next(datetime)