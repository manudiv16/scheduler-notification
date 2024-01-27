import asyncio
from notification_db import NotificationDBHandler
from notification_sender import NotificationSender
from notification_consumer import NotificationConsumer
from notification_detector import Notification_detector

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
        
        rabbit = NotificationSender(
            "notification-senders"
        )
        consumer = NotificationConsumer("notification-request-exchange", "notification-request-queue")
        detector = Notification_detector(clustering=True).run(db,rabbit)
        tasks = [consumer.consume(connection=db),detector]
        loop.run_until_complete(asyncio.gather(*tasks))
        loop.close()
    except KeyboardInterrupt:
        loop.close()
        print("key")


