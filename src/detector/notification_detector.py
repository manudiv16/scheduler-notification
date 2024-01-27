import redis
import asyncio
import logging
import notification as noti

from typing import Any
from utils import loopwait
from datetime import datetime
from dataclasses import dataclass
from notification_db import NotificationDBHandler
from notification_sender import NotificationSender
from returns.result import Failure, Success, safe, Result
from notification import Notification, NotificationStatus, json_to_notification


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("notification_detector")
logging.getLogger("websockets.client").setLevel(logging.WARNING)

@dataclass(frozen=True)
class Notification_detector:
    clustering: bool = False
    
    async def run(self, connection: NotificationDBHandler, sender: NotificationSender) -> None:
        logger.info("Notification detector is running")
        await connection.create_pool()
        await self.__app(connection, sender)

    @loopwait(60)
    async def __app(self, connection: NotificationDBHandler, sender: NotificationSender) -> None:
        async for batch in connection.get_all_notifications_batch():
            await asyncio.gather(*(run(json_to_notification(x), connection, sender, self.clustering) for x in batch))
            
async def run(notification: Result[Notification, Any], connectiondb: NotificationDBHandler, sender: NotificationSender, clustering: bool) -> None:
    match notification:
        case Failure(err):
            logger.error(f"error: {err}")
            logger.info(f"the notify has error")
        case Success(noti):
            now = datetime.now().replace(second=0, microsecond=0)
            status = handler_status(noti, now)
            if clustering:
                redis_client = redis.Redis(host='localhost', port=6379, db=0)
                notification_id_str = str(noti.id)
                if redis_client.set(notification_id_str, 1, ex=10, nx=True):
                    await handler(status, noti, connectiondb, sender, now)
                    redis_client.delete(notification_id_str)
                else:
                    logger.debug(f"the notify {noti.id} is being processed by another worker")
            else:
                await handler(status, noti, connectiondb, sender, now)


async def handler(status: Result[NotificationStatus, Any], notification: Notification, connectiondb: NotificationDBHandler, sender: NotificationSender, now: datetime) -> None:
    match status:
        case Success(NotificationStatus.EXPIRED):
            logger.debug(f"the notify {notification.id} has expired")
            await connectiondb.delete_notification(notification_id=notification.id)
        case Success(NotificationStatus.WAITING):
            logger.debug(f"the notify {notification.id} not match")
        case Success(NotificationStatus.SENDED):
            logger.debug(f"the notify {notification.id} has already been sent")
        case Success(NotificationStatus.SEND):
            if notification.schedule_expression:
                next_time = noti.get_next_time(notification.schedule_expression, now)
                next_time_int: int = int(round(next_time.timestamp())) 
                await connectiondb.update_notification_next_time(notification_id=notification.id, next_time=next_time_int)
            logger.debug(f"the notify {notification.id} will be sent")
            await sender.publish(notification)
        case Failure(error):
            logger.error(f"error: {error}")
            logger.info(f"the notify {notification.id} has error")


@safe
def handler_status(notification: Notification, now: datetime):
        if noti.expired(notification, now):
            return NotificationStatus.EXPIRED
        if not noti.match(notification, now):
            return NotificationStatus.WAITING
        if noti.if_sent(notification, now):
            return NotificationStatus.SENDED
        return NotificationStatus.SEND



if __name__ == "__main__":

    db = NotificationDBHandler(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    rabbit = NotificationSender(
        "notification-senders"
    )

    loop = asyncio.get_event_loop()
    app = Notification_detector(clustering=True).run(db,rabbit)
    # app_ordered = Notification_detector_Ordered().run(db)

    tasks = [app]
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


