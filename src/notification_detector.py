import redis
import asyncio
import logging

from utils import loopwait
from datetime import datetime
from dataclasses import dataclass
from repository import Repository
from returns.future import Future
from typing import Any, Callable, Coroutine
from notification_sender import NotificationSender
from returns.result import Failure, Success, Result, safe
from notification import Notification, NotificationStatus, get_status, get_next_time


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("notification_detector")
logging.getLogger("websockets.client").setLevel(logging.WARNING)

@dataclass(frozen=True)
class Notification_detector:
    clustering: bool = False
    
    async def run(self, connection: Repository, sender: NotificationSender) -> None:
        logger.info("Notification detector is running")
        await self.__app(connection, sender)

    @loopwait(60)
    async def __app(self, connection: Repository, sender: NotificationSender) -> None:
        async for batch in connection.get_all():
            await asyncio.gather(*(self.runner(x, connection, sender, self.clustering) for x in batch))
    
    @classmethod       
    async def runner(cls,notification: Result[Notification, Any], connectiondb: Repository, sender: NotificationSender, clustering: bool) -> None:
        handler_partial = lambda x,y,z: cls.handler(connectiondb, sender, x, y, z)
        await Future.do(
            await cls.run_helper(clustering, handler_partial, notifi) 
            for notifi in notification
        )

    @classmethod
    async def run_helper(cls, clustering: bool, handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], notification: Notification) -> Result[None, Any]:
        try:
            if clustering:
                await cls.run_in_cluster(handler, notification)
                return Success(None)
            else:
                now = datetime.now().replace(second=0, microsecond=0)
                status = get_status(notification, now)
                await handler(status, notification, now)
                return Success(None)
        except Exception as e:
            return Failure(e)


    @classmethod
    async def run_in_cluster(cls, handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], notification: Notification) -> None:
        now = datetime.now().replace(second=0, microsecond=0)
        status = get_status(notification, now)
        redis_client = redis.Redis(host='localhost', port=6379, db=0)
        notification_id_str = str(notification.id)
        if redis_client.set(notification_id_str, 1, ex=10, nx=True):
            await handler(status, notification, now)
            redis_client.delete(notification_id_str)
        else:
            logger.debug(f"the notify {notification.id} is being processed by another worker")
    
    @classmethod
    async def handler(cls, connectiondb: Repository, sender: NotificationSender, status: Result[NotificationStatus, Any], notification: Notification, now: datetime) -> None:
        match status:
            case Success(NotificationStatus.EXPIRED):
                logger.debug(f"the notify {notification.id} has expired")
                await connectiondb.delete(id=notification.id)
            case Success(NotificationStatus.WAITING):
                logger.debug(f"the notify {notification.id} not match")
            case Success(NotificationStatus.SENDED):
                logger.debug(f"the notify {notification.id} has already been sent")
            case Success(NotificationStatus.SEND):
                if notification.schedule_expression:
                    next_time = get_next_time(notification.schedule_expression, now)
                    next_time_int: int = int(round(next_time.timestamp())) 
                    await connectiondb.update(id=notification.id, next_time=next_time_int)
                logger.debug(f"the notify {notification.id} will be sent")
                await sender.publish(notification)
            case Failure(error):
                logger.error(f"error: {error}")
                logger.info(f"the notify {notification.id} has error")

__all__ = ["Notification_detector"]
async def main() -> None:
        from notification_db import NotificationDBHandler
        db = await NotificationDBHandler.create(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
        
        rabbit = NotificationSender(
            "notification-senders"
        )
        detector = Notification_detector(clustering=True).run(db,rabbit)
        tasks = [detector]   
        await asyncio.gather(*tasks) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")

