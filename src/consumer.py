import os
import json
import asyncio
import logging
import aio_pika

from uuid import UUID
from typing import Any, Dict
from aio_pika.pool import Pool
from dataclasses import dataclass
from repository import Repository
from returns.future import FutureResult
from returns.io import IOSuccess, IOFailure
from returns.result import Failure, Success, Result
from opentelemetry.metrics import get_meter_provider
from notification import Notification, json_to_notification
from aio_pika.abc import AbstractRobustConnection, AbstractChannel

meter = get_meter_provider().get_meter("view-name-change", "0.1.2")
register_counter = meter.create_counter("registered_notification_counter")
failed_register_counter = meter.create_counter("failed_registered_notification_counter")
hostname = os.environ.get("HOSTNAME", "localhost")
tag = {"hostname": hostname }
logger = logging.getLogger("notification_consumer")
logger.setLevel(logging.DEBUG)



@dataclass
class NotificationConsumer:
    rabbitmq_request_exchange: str
    rabbitmq_request_queue: str
    amqp_url: str

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
        
    def message_to_notification(self, message: aio_pika.abc.AbstractIncomingMessage) -> Result[Notification, Any]:
        try:
            msg = message.body
            message_dict: Dict[str, Any] = json.loads(msg)
            notifi = json_to_notification(message_dict)
            match notifi:
                case Success(noti):
                    return Success(noti)
                case Failure(err):
                    return Failure(err)
        except Exception as e:
            return Failure(e)
        return Failure("Failed to parse notification")
        
    async def consume(self, connection: Repository) -> None:
        async def send(notification: Notification) -> FutureResult[UUID, Any] :
            return connection.add(object=notification)
        
        async with self.channel_pool.acquire() as channel: 
            while True:
                await channel.set_qos(10)

                queue = await channel.declare_queue(
                    self.rabbitmq_request_queue,
                    durable=True,
                    auto_delete=False
                )
                exchange = await channel.declare_exchange(
                    self.rabbitmq_request_exchange,
                    aio_pika.ExchangeType.DIRECT,
                    durable=True
                )
                # exchange = await channel.get_exchange(self.rabbitmq_request_exchange)
                await queue.bind(exchange, self.rabbitmq_request_queue)

                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        posible_notification: Result[Notification, Any] = self.message_to_notification(message)
                        a = FutureResult.from_result(posible_notification).bind_async(send)
                        match await a:
                            case IOSuccess(Success(notification_id)):
                                register_counter.add(1,tag)
                                logger.info(f"Notification {notification_id} added to database")
                                await message.ack()
                            case IOSuccess(Failure(err)):
                                failed_register_counter.add(1,tag)
                                logger.warning(f"Error adding notification to database: {err}")
                                await message.ack()
                                continue
                            case IOFailure(err):
                                failed_register_counter.add(1,tag)
                                logger.warning(f"Error adding notification to database: {err}")
                                await message.ack()
                                continue
                        #TODO: send to dead letter queue

                await asyncio.sleep(0.1)
            


__all__ = ["NotificationConsumer"]

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    from notification_repository import NotificationDBHandler
    try:
        loop = asyncio.get_event_loop()
        db: Repository = NotificationDBHandler(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='postgress',
                port=5432
            )
        apps = NotificationConsumer("notification-request-exchange", "notification-request-queue", "amqp://guest:guest@rabbitmq:5672")
        tasks = [apps.consume(connection=db),]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
    except KeyboardInterrupt:
        loop.close()
        print("key")