from typing import Any
from handlers import Handler
from dataclasses import dataclass
from repository import Repository
from returns.io import IOResult, IOFailure
from returns.result import Result, Success, Failure
from event import EventAdd, EventUpdate, EventDelete, EventType

@dataclass
class NotificationDBHandler(Handler[EventType]):
    connection: Repository

    async def handle(self, object: Result[EventType,Any]) -> IOResult[str, Any]:
        match object:
            case Success(EventAdd(notification=notification)):
                added = await self.connection.add(object=notification)
                return added.map(lambda _: f"Notification {notification.id} added")\
                            .alt(lambda _: f"Error adding notification {notification.id} to database")
            case Success(EventUpdate(id=id, 
                                     date=date, 
                                    message_body=message_body, 
                                    message_title=message_title, 
                                    expiration_date=expiration_date,
                                    notification_sender=notification_sender, 
                                    schedule_expression=schedule_expression)):
                update = await self.connection.update(id=id, 
                                                      date=date, 
                                                      message_body=message_body, 
                                                      message_title=message_title, 
                                                      expiration_date=expiration_date,
                                                      notification_sender=notification_sender, 
                                                      schedule_expression=schedule_expression
                                                      )
                return update.map(lambda _: f"Notification {id} updated")\
                             .alt(lambda _: f"Error updating notification {id} to database")
            
            case Success(EventDelete(notification_id=notification_id)):
                deleted = await self.connection.delete(id=notification_id)
                return deleted.map(lambda _: f"Notification {notification_id} deleted")\
                                .alt(lambda _: f"Error deleting notification {notification_id} to database")
            case Failure(error):
                return IOFailure(error)
            case _:
                return IOFailure("Invalid message")
            

async def main() -> None:
    import logging
    from handlers import message_to_eventtype
    from notification_repository import NotificationRepository
    from consumer import NotificationConsumer

    logger = logging.getLogger("aiormq.connection").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.connection").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.channel").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.robust_connection").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.pool").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.exchange").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.queue").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.message").setLevel(logging.ERROR)
    logger = logging.getLogger("aio_pika.patterns.rpc").setLevel(logging.ERROR)


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
    apps = NotificationConsumer("notification-request-exchange", "notification-request-queue", "amqp://guest:guest@localhost:5672")
    db_handler: Handler = NotificationDBHandler(connection=db)
    tasks = [apps.consume(transformer=message_to_eventtype, handler=db_handler),]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    import asyncio
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("key")