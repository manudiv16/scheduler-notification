import os
import json
import asyncio
import logging
import aio_pika

from event import Event
from handlers import Handler
from aio_pika.pool import Pool
from dataclasses import dataclass
from returns.result import Result
from typing import Any, Dict, Callable
from returns.io import IOSuccess, IOFailure
from opentelemetry.metrics import get_meter_provider
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
    rabbitmq_dead_letter_exchange: str = "dead-letter-exchange"

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
        
    async def publish_dle(self, msg: bytes) -> None:
        async with self.channel_pool.acquire() as channel:

            try:
                dle = await channel.get_exchange(self.rabbitmq_dead_letter_exchange)
            except Exception:
                await channel.declare_exchange(
                    self.rabbitmq_dead_letter_exchange, 
                    aio_pika.ExchangeType.DIRECT,
                    durable=True
                )
            finally:
                dle = await channel.get_exchange(self.rabbitmq_dead_letter_exchange)
                ready_queue = await channel.declare_queue(
                    self.rabbitmq_dead_letter_exchange, durable=True
                )
                await ready_queue.bind(dle, self.rabbitmq_dead_letter_exchange)
                await dle.publish(
                    aio_pika.Message(
                        body=msg
                    ),
                    routing_key=self.rabbitmq_dead_letter_exchange
                )
                await asyncio.sleep(0.1)
        
    async def consume(self, transformer: Callable[[Dict[str, Any]], Result[Event, Any]], handler: Handler) -> None: 
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
                queue = await channel.get_queue(self.rabbitmq_request_queue)
                exchange = await channel.get_exchange(self.rabbitmq_request_exchange)
                await queue.bind(exchange, self.rabbitmq_request_queue)
                async with queue.iterator() as queue_iter:
                    async for message in queue_iter:
                        body = message.body
                        try:
                            event = json.loads(body)
                            result = await handler.handle(transformer(event))
                            match result:
                                case IOSuccess(a):
                                    logger.info(f"Consumed message: {a.unwrap()}")
                                    await message.ack()
                                case IOFailure(err):
                                    failed_register_counter.add(1,tag)
                                    logger.warning(f"Error consuming: {err.failure()}")
                                    await message.ack()
                                    continue
                                case _:
                                    logger.warning(f"Error consuming: {result}")
                        except Exception as e:
                            failed_register_counter.add(1,tag)
                            await self.publish_dle(body)
                            logger.error(f"Error consuming: {e}")
                            await message.ack()
                            continue
                        

                await asyncio.sleep(0.1)
            


__all__ = ["NotificationConsumer"]
