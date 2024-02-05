import os
import redis
import asyncio
import logging

from utils import loopwait
from datetime import datetime
from dataclasses import dataclass
from repository import Repository
from utils import RedisConfig
from sender import NotificationSender
from typing import Any, Callable, Coroutine, Optional
from returns.result import Failure, Success, Result
from returns.io import IOSuccess, IOFailure
from returns.future import FutureResult
from opentelemetry.metrics import get_meter_provider
from notification import Notification, NotificationStatus, get_status, get_next_time


meter = get_meter_provider().get_meter("view-name-change", "0.1.2")
send_counter = meter.create_counter("send_notification_evaluation_counter")
failed_notification_counter = meter.create_counter("failed_notification_evaluation_counter")
total_notification_counter = meter.create_counter("total_notification_evaluation_counter")
logger = logging.getLogger("notification_detector")
hostname = os.environ.get("HOSTNAME", "localhost")
tag = {"instance": hostname}
logger.setLevel(logging.DEBUG)

@dataclass(frozen=True)
class Notification_detector:
    clustering: bool = False
    redis_config: Optional[RedisConfig] = None

    def __post_init__(self) -> None:
        if self.clustering and not self.redis_config:
            raise ValueError("redis_config is required")
    
    async def run(self, connection: Repository, sender: NotificationSender) -> None:
        logger.info("Notification detector is running")
        await self.__app(connection, sender)

    @loopwait(60)
    async def __app(self, connection: Repository, sender: NotificationSender) -> None:
        async for batch in connection.get_all():
            await asyncio.gather(*(self.runner(x, connection, sender, self.clustering, self.redis_config) for x in batch))
    
    @classmethod       
    async def runner(cls,notification: Result[Notification, Any], connectiondb: Repository, sender: NotificationSender, clustering: bool, redis_config: Optional[RedisConfig]) -> None:
        handler_partial = lambda x,y,z: cls.handler(connectiondb, sender, x, y, z)
        get_current_status_partial = lambda x: cls.get_current_status(x, connectiondb)
        match notification:
            case Success(notify):
                await cls.run_helper(clustering,redis_config ,handler_partial,get_current_status_partial, notify)
            case Failure(error):
                failed_notification_counter.add(1, tag)
                logger.error(f"error: {error}")
                logger.info(f"the notify has error")

    @classmethod
    async def run_helper(cls, clustering: bool, redis_config: Optional[RedisConfig] , handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], get_c_status , notification: Notification) -> Result[None, Any]: #type: ignore
        try:
            if clustering:
                await cls.run_in_cluster(handler, notification, redis_config, get_c_status)
                return Success(None)
            else:
                now = datetime.now().replace(second=0, microsecond=0)
                status = get_status(notification, now)
                await handler(status, notification, now)
                return Success(None)
        except Exception as e:
            return Failure(e)


    @classmethod
    async def get_current_status(cls, notification: Notification, connectiondb: Repository) -> Result[NotificationStatus, Any]: #type: ignore
        current_notification: FutureResult[Notification, Any] = connectiondb.get(id=notification.id)
        match await current_notification:
            case IOSuccess(Success(notify)):
                now = datetime.now().replace(second=0, microsecond=0)
                return get_status(notify, now)
            case Failure(_):
                return NotificationStatus.WAITING

    @classmethod
    async def run_in_cluster(cls, handler: Callable[[Result[NotificationStatus, Any], Notification, datetime], Coroutine[Any, Any, None]], notification: Notification, redis_config: Optional[RedisConfig], get_c_status) -> None: #type: ignore
        now = datetime.now().replace(second=0, microsecond=0)
        if not redis_config:
            raise ValueError("redis_config is required")
        redis_client = redis.Redis(host=redis_config.host, port=redis_config.port, db=redis_config.db)
        notification_id_str = str(notification.id)
        if redis_client.set(notification_id_str, 1, ex=10, nx=True):
            status = await get_c_status(notification)
            await handler(status, notification, now)
            redis_client.delete(notification_id_str)
        else:
            logger.debug(f"the notify {notification.id} is being processed by another worker")
    
    @classmethod
    async def handler(cls, connectiondb: Repository, sender: NotificationSender, status: Result[NotificationStatus, Any], notification: Notification, now: datetime) -> None:
        total_notification_counter.add(1, tag)
        match status:
            case Success(NotificationStatus.EXPIRED):
                logger.debug(f"the notify {notification.id} has expired")
                await connectiondb.delete(id=notification.id)
            case Success(NotificationStatus.WAITING):
                logger.debug(f"the notify {notification.id} not match")
            case Success(NotificationStatus.SENDED):
                logger.debug(f"the notify {notification.id} has already been sent")
            case Success(NotificationStatus.SEND):
                send_counter.add(1, tag)
                if notification.schedule_expression:
                    next_time = get_next_time(notification.schedule_expression, now)
                    next_time_int: int = int(round(next_time.timestamp())) 
                    await connectiondb.update(id=notification.id, next_time=next_time_int)
                logger.debug(f"the notify {notification.id} will be sent")
                await sender.publish(notification)
            case Failure(error):
                failed_notification_counter.add(1, tag)
                logger.error(f"error: {error}")
                logger.info(f"the notify {notification.id} has error")

__all__ = ["Notification_detector"]
async def main() -> None:
        logging.basicConfig(level=logging.DEBUG)
        logging.getLogger().setLevel(logging.DEBUG)
        from notification_repository import NotificationDBHandler
        db = await NotificationDBHandler.create(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
        
        rabbit = NotificationSender(
            "notification-senders",
            "amqp://guest:guest@localhos:5672/"

        )
        redis = RedisConfig(host="localhost", port=6379, db=0)
        detector = Notification_detector(clustering=True,redis_config=redis).run(db,rabbit)
        tasks = [detector]   
        await asyncio.gather(*tasks) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")

