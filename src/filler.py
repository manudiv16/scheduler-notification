import os
import redis
import asyncio
import logging

from utils import loopwait
from dataclasses import dataclass
from repository import Repository
from utils import RedisConfig
from typing import Any, Optional
from sender import NotificationSender
from opentelemetry.metrics import get_meter_provider
from notification import Notification
from returns.result import Result, Success, Failure


meter = get_meter_provider().get_meter("view-name-change", "0.1.2")
send_counter = meter.create_counter("send_notification_evaluation_counter")
failed_notification_counter = meter.create_counter("failed_notification_evaluation_counter")
total_notification_counter = meter.create_counter("total_notification_evaluation_counter")
logger = logging.getLogger("notification_filler")
hostname = os.environ.get("HOSTNAME", "localhost")
tag = {"instance": hostname}
logger.setLevel(logging.DEBUG)

@dataclass(frozen=True)
class Notification_filler:
    clustering: bool = False
    redis_config: Optional[RedisConfig] = None

    def __post_init__(self) -> None:
        if self.clustering and not self.redis_config:
            raise ValueError("redis_config is required")
    
    async def run(self, connection: Repository, sender: NotificationSender) -> None:
        logger.info("Notification filler is running")
        await self.__app(connection, sender)

    @loopwait(minutes=1)
    async def __app(self, connection: Repository, sender: NotificationSender) -> None:
        
        if not self.redis_config:
            raise ValueError("redis_config is required")
        
        redis_client = redis.Redis(host=self.redis_config.host, port=self.redis_config.port, db=self.redis_config.db)
        if redis_client.set("semaphore", 1, ex=10, nx=True):

            logger.debug(f"Starting the filler...")
            async for batch in connection.get_all(batch_size=100):
                await asyncio.gather(*(self.filler(x, sender) for x in batch))
            logger.debug(f"DONE!")
            redis_client.delete("semaphore")
            
        else:
            logger.debug(f"the filler is being processed by another worker")

    async def filler(self, notification: Result[Notification,Any] , sender: NotificationSender) -> None:
        match notification:
            case Success(n):
                await sender.publish(n)
            case _:
                logger.debug(f"Notification is invalid")

        

async def main() -> None:
    import logging
    from notification_repository import NotificationRepository
    from consumer import NotificationConsumer


    logger = logging.getLogger("notification_consumer")
    logger.setLevel(logging.DEBUG)
    logging.basicConfig(level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")


    logger = logging.getLogger("notification_handler")
    logger.setLevel(logging.DEBUG)

    db: Repository = await NotificationRepository.create(
                    dbname='postgres',
                    user='postgres',
                    password='postgres',
                    host='localhost',
                    port=5432
                )
    
    sender = NotificationSender("notification-to-consume", "amqp://guest:guest@localhost:5672")
    filler = Notification_filler(clustering=True, redis_config=RedisConfig(host="localhost", port=6379, db=0))
    tasks = [filler.run(connection=db, sender=sender)]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except Exception as e:
        print(e)
        raise e
