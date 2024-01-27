import asyncio
import aio_pika
from aio_pika.pool import Pool
from aio_pika import Channel
import json
from typing import Any
from notification_db import NotificationDBHandler
from notification import json_to_notification

RABBIT_URI = "amqp://guest:guest@localhost/"


class NotificationConsumer:
    def __init__(self, rabbitmq_request_exchange:str, rabbitmq_request_queue: str, amqp_url: str = RABBIT_URI):
        self.rabbitmq_request_exchange = rabbitmq_request_exchange
        self.rabbitmq_request_queue = rabbitmq_request_queue
        self.amqp_url = amqp_url
        self.loop = asyncio.get_event_loop()
        self.connection_pool = Pool(self.get_connection, max_size=2, loop=self.loop)
        self.channel_pool = Pool(self.get_channel, max_size=10, loop=self.loop)


    async def get_connection(self):
        return await aio_pika.connect_robust(self.amqp_url)
    
    async def get_channel(self) -> Channel:
        async with self.connection_pool.acquire() as connection:
            return await connection.channel()
        
    async def consume(self, connection: NotificationDBHandler) -> None:
        await connection.create_pool()
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
                        match notifi:
                            case (None, err):
                                print(err)
                            case notifi:
                                print("ok")
                                await connection.insert_notification(notifi)
                        # await publish(msg)
                        await message.ack()

                await asyncio.sleep(0.1)
        


if __name__ == "__main__":
    try:
        loop = asyncio.get_event_loop()
        db = NotificationDBHandler(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port='5432'
            )
        apps = NotificationConsumer("notification-request-exchange", "notification-request-queue")
        tasks = [apps.consume(connection=db),]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
    except KeyboardInterrupt:
        loop.close()
        print("key")