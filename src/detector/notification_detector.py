from dataclasses import dataclass
from typing import Union, Tuple
from notification_sender import NotificationSender
from notification import Notification, NotificationStatus, json_to_notification
import notification as noti
from notification_db import NotificationDBHandler
from datetime import datetime
from utils import loopwait
import asyncio
import logging
import redis


ThrowNotification = Union[NotificationStatus, Tuple[NotificationStatus, Exception]]
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("notification_detector")
logging.getLogger("websockets.client").setLevel(logging.WARNING)

@dataclass(frozen=True)
class Notification_detector:
    clustering: bool = False
    
    async def run(self, connection: NotificationDBHandler, sender: NotificationSender):
        logger.debug("Notification detector is running")
        await connection.create_pool()
        await self.__app(connection, sender)

    @loopwait(60)
    async def __app(self, connection: NotificationDBHandler, sender: NotificationSender):
        async for batch in connection.get_all_notifications_batch():
            notifications = map(lambda x : json_to_notification(x), batch)
            await asyncio.gather(*(run(x, connection, sender, self.clustering) for x in notifications))
            
async def run(notification: Notification, connectiondb: NotificationDBHandler, sender: NotificationSender, clustering: bool) -> None:
    now = datetime.now().replace(second=0, microsecond=0)
    status = handler_status(notification, now)
    if clustering:
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        notification_id_str = str(notification.id)
        if redis_client.set(notification_id_str, 1, ex=10, nx=True):
            await handler(status, notification, connectiondb, sender, now)
            redis_client.delete(notification_id_str)
        else:
            logger.debug(f"the notify {notification.id} is being processed by another worker")
    else:
        await handler(status, notification, connectiondb, sender, now)

async def handler(status: ThrowNotification, notification: Notification, connectiondb: NotificationDBHandler, sender: NotificationSender, now: datetime) -> None:
    match status:
        case NotificationStatus.EXPIRED:
            logger.debug(f"the notify {notification.id} has expired")
            await connectiondb.delete_notification(notification_id=notification.id)
        case NotificationStatus.WAITING:
            logger.debug(f"the notify {notification.id} not match")
        case NotificationStatus.SENDED:
            logger.debug(f"the notify {notification.id} has already been sent")
        case NotificationStatus.SEND:
            if notification.schedule_expression:
                next_time = noti.get_next_time(notification.schedule_expression, now)
                next_time = int(round(next_time.timestamp()))
                await connectiondb.update_notification_next_time(notification_id=notification.id, next_time=next_time)
            logger.debug(f"the notify {notification.id} will be sent")
            await sender.publish(notification)
        case (NotificationStatus.ERROR, error):
            logger.error(f"error: {error}")
            logger.debug(f"the notify {notification.id} has error")



def handler_status(notification: Notification, now: datetime) -> ThrowNotification:
    try:
        if noti.expired(notification, now):
            return NotificationStatus.EXPIRED
        if not noti.match(notification, now):
            return NotificationStatus.WAITING
        if noti.if_sent(notification, now):
            return NotificationStatus.SENDED
        return NotificationStatus.SEND
    except Exception as e:
        return (NotificationStatus.ERROR,e)



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


