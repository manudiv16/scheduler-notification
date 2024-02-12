import asyncpg

from uuid import UUID
from repository import Repository
from contextlib import asynccontextmanager
from returns.result import Result, Failure
from typing import Any, AsyncIterator, List, Optional
from notification import Notification, json_to_notification
from returns.future import FutureResult, future_safe, future

class NotificationRepository(Repository[Notification]): # type: ignore
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool: Optional[asyncpg.Pool[asyncpg.Record]] = None

    @classmethod
    async def create(cls, dbname: str, user: str, password: str, host: str, port: int) -> 'NotificationRepository':
        self = cls(dbname, user, password, host, port)
        await self.create_pool()
        return self

    @asynccontextmanager
    async def connect(self) -> AsyncIterator[asyncpg.Connection]:  
        if self.pool:
            async with self.pool.acquire() as connection:
                yield connection # type: ignore
        else:
            raise Exception("No pool created")

    async def create_pool(self) -> None:
        self.pool = await asyncpg.create_pool(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    async def close_pool(self) -> None:
        if self.pool is not None:
            await self.pool.close()
            self.pool = None

    async def create_notification_table(self) -> None:
        create_table_query = '''
            CREATE TABLE IF NOT EXISTS notifications (
                id UUID PRIMARY KEY,
                message_body TEXT,
                message_title TEXT,
                notification_sender TEXT,
                schedule_expression TEXT,
                date TIMESTAMP,
                expiration_date TIMESTAMP
            );
        '''
        async with self.connect() as connection: 
            await connection.execute(create_table_query)

    @future_safe
    async def add(self, object: Notification) -> UUID:
        insert_query = '''
            INSERT INTO notifications (
                id, message_body, message_title, notification_sender, schedule_expression, date, expiration_date
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7);
        '''
        async with self.connect() as connection:
            await connection.execute(insert_query, 
                object.id, object.message_body, object.message_title, object.notification_sender,
                object.schedule_expression, object.date, object.expiration_date
            )
            return object.id
        
    def get(self, id: UUID) -> FutureResult[Notification, Any]:
        return FutureResult.from_typecast(self._get(id))

    @future
    async def _get(self, id: UUID) -> Result[Notification, Any]:
        select_query = '''
            SELECT * FROM notifications
            WHERE id = $1;
        '''
        try:
            async with self.connect() as connection:
                row = await connection.fetchrow(select_query, id)
                if row is None:
                    return Failure(f"Notification not found in database, id: {id}")
                else:
                    return json_to_notification(row)
        except Exception as e:
            return Failure(e)

    # refactor to use cursor instead of offset
    async def get_all(self, batch_size: int = 10) -> AsyncIterator[List[Result[Notification, Any]]]: 
        offset = 0
        async with self.connect() as connection:
            while True:
                query = "SELECT * FROM notifications LIMIT $1 OFFSET $2"
                batch = await connection.fetch(query, batch_size, offset)
                if not batch:
                    break
                notifications = [ json_to_notification(x) for x in batch]
                yield notifications
                offset += batch_size

    @future_safe
    async def update(self, id: UUID, **kwargs: object) -> UUID:
        kwargs_non_none = {key: value for key, value in kwargs.items() if value is not None}
        update_query = "UPDATE notifications SET "
        update_query += ", ".join(f"{key} = ${i+2}" for i, key in enumerate(kwargs_non_none.keys()))
        update_query += " WHERE id = $1 RETURNING *;"
        async with self.connect() as connection:
            id_updated = await connection.fetchval(update_query, id, *kwargs_non_none.values())
            if id_updated is None:
                raise Exception("Notification not found")
            return UUID(str(id_updated))
    
    @future_safe
    async def delete(self, id: UUID) -> UUID:
        delete_query = '''
            DELETE FROM notifications
            WHERE id = $1 RETURNING id;
        '''
        async with self.connect() as connection:
            id_deleted = await connection.fetchval(delete_query, id)
            if id_deleted is None:
                raise Exception("Notification not found")
            return UUID(str(id_deleted))

    @future_safe
    async def delete_all(self) -> str:
        delete_query = '''
            DELETE FROM notifications;
        '''
        async with self.connect() as connection:
            return await connection.execute(delete_query)

__all__ = ["NotificationRepository"]

async def main() -> None:
    from datetime import datetime
    import uuid
    db = await NotificationRepository.create(
                dbname='postgres',
                user='postgres',
                password='postgres',
                host='localhost',
                port=5432
            )
    await db.create_notification_table()
    # await db.delete_all()
    for i in range(1200):
        await db.add(Notification(
            id=uuid.uuid4(),
            message_body="send this message every 2 minutes",
            message_title="title",
            notification_sender="sender",
            schedule_expression="*/2 * * * *"
            # next_time=0,
            # date = datetime.strptime("2021-10-10 10:10:10", "%Y-%m-%d %H:%M:%S"),
            # expiration_date= datetime.strptime("2021-10-10 10:10:10", "%Y-%m-%d %H:%M:%S")
        ))
    a = []
    async for notifications in db.get_all():
        for notification in notifications:
            a.append(notification)
    # print(a)
    


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
