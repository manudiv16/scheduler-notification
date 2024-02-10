import os 
import json
import asyncio
import logging
import aio_pika

from typing import Any, Dict
from aio_pika.pool import Pool
from notification import Notification
from aio_pika.abc import AbstractQueue
from aio_pika.abc import AbstractChannel
from aio_pika.abc import AbstractExchange
from aio_pika.abc import AbstractRobustConnection
from opentelemetry.metrics import get_meter_provider
from event import EventDelete, SendableEventType, EventSend
from returns.future import FutureResult, future_safe


meter = get_meter_provider().get_meter("view-name-change", "0.1.2")
sended_counter = meter.create_counter("send_notification_counter")
failed_notification_counter = meter.create_counter("failed_notification_counter")

# RABBIT_URI = "amqp://guest:guest@localhost/"
DEAD_LETTER_EXCHANGE = "dead-letter-exchange"
hostname = os.environ.get("HOSTNAME", "localhost")
logger = logging.getLogger("notification_sender")
logging.getLogger("aio_pika.connection").setLevel(logging.WARNING)
logging.getLogger("aio_pika.exchange").setLevel(logging.WARNING)
logging.getLogger("aio_pika.channel").setLevel(logging.WARNING)
logging.getLogger("aio_pika.robust_connection").setLevel(logging.WARNING)
logging.getLogger("aiormq.connection").setLevel(logging.WARNING)
logging.getLogger("aio_pika.queue").setLevel(logging.WARNING)
logging.getLogger("aio_pika.pool").setLevel(logging.WARNING)
logger.setLevel(logging.DEBUG)
tag = {"instance": hostname}

class NotificationSender:

    def __init__(self, exchange:str, amqp_url: str, dead_letter_exchange: str = DEAD_LETTER_EXCHANGE):
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
        
    
    def _build_event(self, event: SendableEventType) -> Dict[str, Any]:
        match event:
            case EventDelete(notification_id=notification_id):
                return {"command": "delete", "id": str(notification_id)}
            case EventSend(notification=notification):
                return notification.send_dict()
            
    async def __publish(self, msg: bytes, exchange: str, routing_key: str) -> None:
        async with self.channel_pool.acquire() as channel: 
            print(f"Publishing to {exchange} with routing key {routing_key}")
            ex: AbstractExchange = await channel.declare_exchange(exchange, durable=True)
            queue: AbstractQueue = await channel.declare_queue(routing_key, durable=True)
            await queue.bind(ex, routing_key)
            await ex.publish(
                aio_pika.Message(
                    body=msg,
                    delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                ),
                routing_key
            )
            await asyncio.sleep(0.1)
            
    @future_safe
    async def publish(self, event: SendableEventType) -> str:
        command = self._build_event(event)
        print(f"Publishing event {event}")
        print(f"Command to publish {command}")
        noti = json.dumps(command, indent=2).encode('utf-8')
        print(f"Notification to publish {noti}")
        match event:
            case EventDelete(_):
                await self.__publish(noti, "notification-request-exchange", "notification-request-queue")
                return f"Event {event} published"
            case EventSend(notification=notification):
                await self.__publish(noti, self.exchange, notification.notification_sender)
                return f"Event {event} published"


__all__ = ["NotificationSender"]