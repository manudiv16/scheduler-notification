from asyncio.exceptions import InvalidStateError
from contextlib import asynccontextmanager
from uuid import UUID
import asyncpg
from returns.result import Result, Success, Failure
from returns.future import future_safe
from typing import Any, AsyncGenerator, AsyncIterator, List, Optional
from notification import Notification, json_to_notification
import datetime

class NotificationDBHandler:
    def __init__(self, dbname: str, user: str, password: str, host: str, port: int) -> None:
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool: Optional[asyncpg.Pool[asyncpg.Record]] = None


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
                print('insertado')
                return Success(notification.id)
        except asyncpg.UniqueViolationError as e:
            # Handle unique constraint violation (if needed)
            print(f'error: {e}')
            return Failure(e)

    @future_safe
    async def get_notifications(self, user_id: UUID) -> Result[List[Result[Notification, Any]], Any]:
        select_query = '''
            SELECT * FROM notifications
            WHERE user_id = $1;
        '''
        async with self.connect() as connection:
            notifications = []
            row = await connection.fetchrow(select_query, user_id)
            if row is None:
                return Success([])
            else:
                notifications.append(json_to_notification(row))
                row = await connection.fetchrow(select_query, user_id)
            return Success(notifications)
            

    async def update_notification(self, notification_id: str, message_body: str, message_title: str, notification_sender: str, 
                                  schedule_expression: str, next_time: Optional[int] = None, date: Optional[datetime] = None, expiration_date: Optional[datetime] = None) -> None: # type: ignore
        update_query = '''
            UPDATE notifications
            SET message_body = $1, message_title = $2, notification_sender = $3, 
                schedule_expression = $4, next_time = $5, date = $6, expiration_date = $7
            WHERE id = $8;
        '''
        async with self.connect() as connection:
            await connection.execute(update_query, 
                message_body, message_title, notification_sender, schedule_expression,
                next_time, date, expiration_date, notification_id
            )
    
    async def update_notification_next_time(self, notification_id: UUID, next_time: int) -> None:
        update_query = '''
            UPDATE notifications
            SET next_time = $1
            WHERE id = $2;
        '''
        async with self.connect() as connection:
            await connection.execute(update_query, next_time, notification_id)

    async def delete_notification(self, notification_id: UUID) -> None:
        delete_query = '''
            DELETE FROM notifications
            WHERE id = $1;
        '''
        async with self.connect() as connection:
            await connection.execute(delete_query, notification_id)

    async def delete_all_notifications(self) -> None:
        delete_query = '''
            DELETE FROM notifications;
        '''
        async with self.connect() as connection:
            await connection.execute(delete_query)

    @future_safe
    async def get_all_notifications(self) -> List[Result[Notification, Any]]:
        select_query = '''
            SELECT * FROM notifications;
        '''
        async with self.connect() as connection:
            row = await connection.fetch(select_query)
            notifications   = [ json_to_notification(x) for x in row ]
            return notifications
    
    async def get_all_notifications_batch(self, batch_size: int = 10) -> AsyncGenerator[List[Result[Notification, Any]], None]:
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


