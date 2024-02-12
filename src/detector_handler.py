from typing import Any, Dict
from handlers import Handler
from datetime import datetime
from returns.result import Result
from dataclasses import dataclass
from notification import Notification
from sender import NotificationSender
from returns.future import future_safe
from returns.io import IOResult, IOFailure
from returns.result import Success, Failure
from notification import json_to_notification
from notification import NotificationStatus, get_status
from event import EventDelete, SendableEventType, EventSend

@dataclass
class NotificationDetector(Handler[Notification]):
    sender: NotificationSender
        
    async def handle(self, object: Result[Notification, Any]) -> IOResult[str, Any]:
        now = datetime.now().replace(second=0, microsecond=0)
        a = object.bind(lambda x: get_status(x, now).map(lambda y: (x, y)))
        match a:
            case Success((notifi, status)):
                return await self.consume_notification(notifi, status)
            case Failure(error):
                return IOFailure(error)
            case _:
                return IOFailure("Invalid message")

    @future_safe
    async def consume_notification(self, notification: Notification, status: NotificationStatus) -> str:
        match status:
            case NotificationStatus.EXPIRED as expired:
                event_delete = self._create_event(notification, expired)
                await self.sender.publish(event_delete) 
                return f"Notification {notification.id} is expired"
            case NotificationStatus.WAITING:
                return f"Notification {notification.id} is waiting"
            case NotificationStatus.SEND as send:
                event_send = self._create_event(notification, send)
                await self.sender.publish(event_send)
                return f"Notification {notification.id} is send"

    def _create_event(self, notification: Notification, status: NotificationStatus) -> SendableEventType:
        match status:
            case NotificationStatus.EXPIRED:
                return EventDelete(notification_id=notification.id)
            case NotificationStatus.SEND:
                return EventSend(notification=notification)
            case _:
                return EventDelete(notification_id=notification.id)

def message_to_notification(message: Dict[str, Any])  -> Result[Notification, Any]:
        return json_to_notification(message)

async def main() -> None:
    import logging
    from consumer import NotificationConsumer

    
    logger = logging.getLogger("notification_consumer")
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    logger = logging.getLogger("notification_handler")
    logger.setLevel(logging.DEBUG)

    apps = NotificationConsumer("notification-to-consume", "notification-to-consume", "amqp://guest:guest@localhost:5672")
    sender = NotificationSender("notification-to-send", "amqp://guest:guest@localhost:5672")
    detector_handler: Handler = NotificationDetector(sender=sender)
    tasks = [apps.consume(transformer=message_to_notification,handler=detector_handler)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")