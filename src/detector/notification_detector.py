import redis
import asyncio
import logging
import notification as noti

from utils import loopwait
from datetime import datetime
from dataclasses import dataclass
from returns.future import Future
from typing import Any, Callable, Coroutine
from notification_db import NotificationDBHandler
from notification_sender import NotificationSender
from returns.result import Failure, Success, Result
from notification import Notification, NotificationStatus, get_status
from typing import Coroutine


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
            await asyncio.gather(*(run(x, connection, sender, self.clustering) for x in batch))
            
async def run(notification: Result[Notification, Any], connectiondb: NotificationDBHandler, sender: NotificationSender, clustering: bool) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    handler_partial = lambda x,y,z: handler(connectiondb, sender, x, y, z)
    await Future.do(
        await run_helper(clustering, handler_partial, notification) #type: ignore 
        for notification in notification
    )

async def run_helper(clustering: bool, handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], notification: Notification) -> None:
    if clustering:
        await run_in_cluster(handler, notification)
    else:
        now = datetime.now().replace(second=0, microsecond=0)
        status = get_status(notification, now)
        await handler(status, notification, now)

async def run_in_cluster(handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], notification: Notification) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    status = get_status(notification, now)
    redis_client = redis.Redis(host='localhost', port=6379, db=0)
    notification_id_str = str(notification.id)
    if redis_client.set(notification_id_str, 1, ex=10, nx=True):
        await handler(status, notification, now)
        redis_client.delete(notification_id_str)
    else:
        logger.debug(f"the notify {notification.id} is being processed by another worker")

async def handler(connectiondb: NotificationDBHandler, sender: NotificationSender, status: Result[NotificationStatus, Any], notification: Notification, now: datetime) -> None:
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


if __name__ == "__main__":

    db = NotificationDBHandler(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port=5432
    )

    rabbit = NotificationSender(
        "notification-senders"
    )

    loop = asyncio.get_event_loop()
    app = Notification_detector(clustering=True).run(db,rabbit)

    tasks = [app]
    loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


