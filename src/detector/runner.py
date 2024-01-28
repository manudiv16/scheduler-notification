import asyncio
from notification_db import NotificationDBHandler
from notification_sender import NotificationSender
from notification_consumer import NotificationConsumer
from notification_detector import Notification_detector

async def main() -> None:
        db = NotificationDBHandler(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
        
        rabbit = NotificationSender(
            "notification-senders"
        )
        consumer = NotificationConsumer("notification-request-exchange", "notification-request-queue")
        detector = Notification_detector(clustering=True).run(db,rabbit)
        tasks = [consumer.consume(connection=db),detector]   
        await asyncio.gather(*tasks) 

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")


