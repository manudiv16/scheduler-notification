import json
import asyncio
import logging
import aio_pika

from typing import Any
from aio_pika import Queue
from aio_pika.pool import Pool
from notification import Notification
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractRobustConnection
from aio_pika.abc import AbstractChannel


RABBIT_URI = "amqp://guest:guest@localhost/"
DEAD_LETTER_EXCHANGE = "dead-letter-exchange"

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("notification_sender")
logging.getLogger("aio_pika.connection").setLevel(logging.WARNING)
logging.getLogger("aio_pika.exchange").setLevel(logging.WARNING)
logging.getLogger("aio_pika.channel").setLevel(logging.WARNING)
logging.getLogger("aio_pika.robust_connection").setLevel(logging.WARNING)
logging.getLogger("aiormq.connection").setLevel(logging.WARNING)
logging.getLogger("aio_pika.queue").setLevel(logging.WARNING)
logging.getLogger("aio_pika.pool").setLevel(logging.WARNING)

class NotificationSender:

    def __init__(self, exchange:str, amqp_url: str = RABBIT_URI, dead_letter_exchange: str = DEAD_LETTER_EXCHANGE):
        self.exchange = exchange
        self.amqp_url = amqp_url
        self.dead_letter_exchange = dead_letter_exchange
        self.loop = asyncio.get_event_loop()
        self.connection_pool: Pool[AbstractRobustConnection] = Pool(self.get_connection, max_size=2, loop=self.loop)
        self.channel_pool: Pool[aio_pika.Channel] = Pool(self.get_channel, max_size=10, loop=self.loop)

    async def get_connection(self) -> AbstractRobustConnection:
        return await aio_pika.connect_robust(self.amqp_url)
    
    async def get_channel(self) -> AbstractChannel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel() 
        
    async def publish(self, notification: Notification) -> None:
        async with self.channel_pool.acquire() as channel:  
            routing_key = notification.notification_sender

            exchange: AbstractExchange = await channel.declare_exchange(self.exchange, durable=True)
            dle: AbstractExchange = await channel.declare_exchange(self.dead_letter_exchange, durable=True)

            ready_queue: AbstractQueue = await channel.declare_queue(
                routing_key, durable=True
            )
            dead_letter_queue: AbstractQueue = await channel.declare_queue(
                self.dead_letter_exchange, durable=True
            )

            await dead_letter_queue.bind(self.dead_letter_exchange)
            await ready_queue.bind(exchange, routing_key)


            try:
                body: dict[str, Any]= {"title": notification.message_title, "body": notification.message_body}

                body_json = json.dumps(body).encode("utf-8")
                await exchange.publish(
                    aio_pika.Message(
                        body=body_json,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key
                )
            except Exception as e:
                logger.error(f"error: {e}")
                await dle.publish(
                    aio_pika.Message(
                        body=body_json,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key
                )

__all__ = ["NotificationSender"]