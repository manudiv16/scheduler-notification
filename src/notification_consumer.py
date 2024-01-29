import json
import asyncio
import logging
import aio_pika
from uuid import UUID
from aio_pika.pool import Pool
from dataclasses import dataclass
from typing import Coroutine, Any
from repository import Repository
from returns.future import Future
from returns.result import Failure, Success, Result
from notification import Notification, json_to_notification
from aio_pika.abc import AbstractRobustConnection, AbstractChannel

RABBIT_URI = "amqp://guest:guest@localhost/"

logging.basicConfig(level=logging.ERROR, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("notification_consumer")
logging.getLogger("websockets.client").setLevel(logging.WARNING)

@dataclass
class NotificationConsumer:
    rabbitmq_request_exchange: str
    rabbitmq_request_queue: str
    amqp_url: str = RABBIT_URI

    def __post_init__(self) -> None:
        self.loop = asyncio.get_event_loop()
        self.connection_pool: Pool[AbstractRobustConnection] = Pool(self.get_connection, max_size=2, loop=self.loop)
        self.channel_pool: Pool[AbstractChannel] = Pool(self.get_channel, max_size=10, loop=self.loop)
        logger.info("Notification consumer is running")


    async def get_connection(self) -> AbstractRobustConnection:
        return await aio_pika.connect_robust(self.amqp_url)

    async def get_channel(self) -> AbstractChannel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()
        
    async def consume(self, connection: Repository) -> None:
        def send(notification: Notification) -> Coroutine[Any, Any, Result[UUID, Any]]:
            return connection.add(object=notification)
        async with self.channel_pool.acquire() as channel: 
            while True:
                await channel.set_qos(10)

                queue = await channel.declare_queue(
                    self.rabbitmq_request_queue,
                    durable=True,
                    auto_delete=False
                )
                exchange = await channel.get_exchange(self.rabbitmq_request_exchange)
                await queue.bind(exchange, self.rabbitmq_request_queue)

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        msg = message.body
                        msg = json.loads(msg)
                        notifi = json_to_notification(msg)
                        hola =  await Future.do(
                            await send(notifi)
                            for notifi in notifi
                        )
                        hola.map(self.print_result)
                        await message.ack()

                await asyncio.sleep(0.1)
    @staticmethod
    def print_result(result: Result[UUID, Any]) -> None:
        match result:
            case Failure(err):
                logger.info(f"Error adding notification to database: {err}")
            case Success(noti):
                logger.info(f"Notification {noti} added to database")


__all__ = ["NotificationConsumer"]

if __name__ == "__main__":
    from notification_db import NotificationDBHandler
    try:
        loop = asyncio.get_event_loop()
        db: Repository = NotificationDBHandler(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
        apps = NotificationConsumer("notification-request-exchange", "notification-request-queue")
        tasks = [apps.consume(connection=db),]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
    except KeyboardInterrupt:
        loop.close()
        print("key")