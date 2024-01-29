import asyncpg
from uuid import UUID
from repository import Repository
from contextlib import asynccontextmanager
from returns.result import Result, Success, Failure
from notification import Notification, json_to_notification
from typing import Any, AsyncGenerator, AsyncIterator, List, Optional

class NotificationDBHandler(Repository[Notification]):
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool: Optional[asyncpg.Pool[asyncpg.Record]] = None

    @classmethod
    async def create(cls, dbname: str, user: str, password: str, host: str, port: int) -> 'NotificationDBHandler':
        self = cls(dbname, user, password, host, port)
        await self.create_pool()
        return self


    @asynccontextmanager
    async def connect(self) -> AsyncIterator[asyncpg.Connection]:  # type: ignore
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
                user_id UUID,
                next_time INTEGER,
                date TIMESTAMP,
                expiration_date TIMESTAMP
            );
        '''
        async with self.connect() as connection: 
            await connection.execute(create_table_query)

    async def add(self, notification: Notification) -> Result[UUID, Any]:
        insert_query = '''
            INSERT INTO notifications (
                id, message_body, message_title, notification_sender, schedule_expression, user_id,
                next_time, date, expiration_date
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
        '''
        try:
            async with self.connect() as connection:
                await connection.execute(insert_query, 
                    notification.id, notification.message_body, notification.message_title, notification.notification_sender,
                    notification.schedule_expression, notification.user_id, notification.next_time, notification.date, notification.expiration_date
                )
                return Success(notification.id)
        except asyncpg.UniqueViolationError as e:
            return Failure(e)

    async def get(self, id: UUID) -> Result[Notification, Any]:
        select_query = '''
            SELECT * FROM notifications
            WHERE id = $1;
        '''
        try:
            async with self.connect() as connection:
                row = await connection.fetchrow(select_query, id)
                if row is None:
                    return Success()
                else:
                    return Success(json_to_notification(row))
        except Exception as e:
            return Failure(e)
    
    async def get_all(self, batch_size: int = 10) -> AsyncGenerator[List[Result[Notification, Any]], None]:
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

    async def update(self, id: UUID, **kwargs) -> None:
        update_query = "UPDATE notifications SET "
        update_query += ", ".join(f"{key} = ${i+2}" for i, key in enumerate(kwargs))
        update_query += " WHERE id = $1"

        async with self.connect() as connection:
            await connection.execute(update_query, id, *kwargs.values())

    async def delete(self, notification_id: UUID) -> None:
        delete_query = '''
            DELETE FROM notifications
            WHERE id = $1;
        '''
        async with self.connect() as connection:
            await connection.execute(delete_query, notification_id)

    async def delete_all(self) -> None:
        delete_query = '''
            DELETE FROM notifications;
        '''
        async with self.connect() as connection:
            await connection.execute(delete_query)

__all__ = ["NotificationDBHandler"]