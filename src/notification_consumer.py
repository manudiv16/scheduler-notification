import asyncio
import aio_pika
from aio_pika.pool import Pool
from aio_pika.exchange import Exchange
from aio_pika.message import Message
from aio_pika import Channel
from aio_pika import Queue
import json
from typing import Any

RABBIT_URI = "amqp://guest:guest@localhost/"
DEAD_LETTER_EXCHANGE = "dead-letter-exchange"

class NotificationConsumer:

    def __init__(self, rabbitmq_request_exchange:str, rabbitmq_request_queue: str, rabbitmq_schedule_exchange: str,amqp_url: str = RABBIT_URI, dead_letter_exchange: str = DEAD_LETTER_EXCHANGE):
        self.rabbitmq_request_exchange = rabbitmq_request_exchange
        self.rabbitmq_request_queue = rabbitmq_request_queue
        self.rabbitmq_schedule_exchange = rabbitmq_schedule_exchange
        self.amqp_url = amqp_url
        self.dead_letter_exchange = dead_letter_exchange
        self.loop = asyncio.get_event_loop()
        self.connection_pool = Pool(self.get_connection, max_size=2, loop=self.loop)
        self.channel_pool = Pool(self.get_channel, max_size=10, loop=self.loop)
        self.task = self.loop.create_task(self.consume())

    async def get_connection(self):
        return await aio_pika.connect_robust(self.amqp_url)
    
    async def get_channel(self) -> aio_pika.Channel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()
        
    async def consume(self) -> None:
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
                        print(msg)
                        
                        db_connection = db.partial_connection()

                        print(f"message user_id: {msg['user_id']}")
                        await db_connection(db.create_notification, user_id = msg["user_id"], notification_id = msg["notification_id"], notification_sender = msg["notification_sender"], schedule_expression = msg["schedule_expression"],message =  msg["message"] )
                        # await publish(msg)
                        await message.ack()

                await asyncio.sleep(0.1)
        
    async def publish(self, msg: str | bytes | bytearray) -> None:
        async with self.channel_pool.acquire() as channel:  
            routing_key = "test"

            exchange: Exchange = await channel.declare_exchange(self.rabbitmq_schedule_exchange, durable=True)
            dle: Exchange = await channel.declare_exchange(self.dead_letter_exchange, durable=True)

            ready_queue: Queue = await channel.declare_queue(
                routing_key, durable=True
            )
            dead_letter_queue: Queue = await channel.declare_queue(
                "dead-letter-queue.notification", durable=True
            )

            await dead_letter_queue.bind(self.dead_letter_exchange, routing_key)
            await ready_queue.bind(exchange, routing_key)


            try:
                body: dict[str, Any] = json.loads(msg)

                body_json = json.dumps(body).encode("utf-8")
                await exchange.publish(
                    aio_pika.Message(
                        body=body_json,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key
                )
            except Exception as e:
                print(e)
                print("Message is not valid")
                await dle.publish(
                    aio_pika.Message(
                        body=msg,
                        delivery_mode=aio_pika.DeliveryMode.PERSISTENT
                    ),
                    routing_key
                )



if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        apps = NotificationConsumer("notification-request-exchange", "notification-request-queue", "notification-schedule-exchange")
        tasks = [apps.consume(),]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
    except KeyboardInterrupt:
        loop.close()
        print("key")