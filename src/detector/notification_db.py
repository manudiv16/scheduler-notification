import random
import asyncpg
import asyncio
from asyncpg import exceptions

class NotificationDBHandler:
    def __init__(self, dbname, user, password, host, port):
        self.dbname = dbname
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.pool = None

    def __enter__(self):
        self.pool = self.create_pool()
        return self
 
    def __exit__(self, *args):
        self.close_pool()

    async def create_pool(self):
        self.pool = await asyncpg.create_pool(
            database=self.dbname,
            user=self.user,
            password=self.password,
            host=self.host,
            port=self.port
        )

    async def close_pool(self):
        await self.pool.close()

    async def create_notification_table(self):
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
        async with self.pool.acquire() as connection:
            await connection.execute(create_table_query)

    async def insert_notification(self, notification_id, message_body, message_title, notification_sender, schedule_expression, user_id,
                                  next_time=None, date=None, expiration_date=None):
        insert_query = '''
            INSERT INTO notifications (
                id, message_body, message_title, notification_sender, schedule_expression, user_id,
                next_time, date, expiration_date
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9);
        '''
        try:
            async with self.pool.acquire() as connection:
                await connection.execute(insert_query, 
                    notification_id, message_body, message_title, notification_sender,
                    schedule_expression, user_id, next_time, date, expiration_date
                )
            return notification_id
        except exceptions.UniqueViolationError:
            # Handle unique constraint violation (if needed)
            return None
        
    async def get_first_notification_by_next_time(self, time=1):
        select_query = '''
            SELECT * FROM notifications
            ORDER BY next_time
            LIMIT 1;
        '''
        async with self.pool.acquire() as connection:
            row = await connection.fetchrow(select_query)
            return row


    async def get_notifications(self, user_id) -> [dict[str, str]]:
        select_query = '''
            SELECT * FROM notifications
            WHERE user_id = $1;
        '''
        async with self.pool.acquire() as connection:
            return await connection.fetch(select_query, user_id)

    async def update_notification(self, notification_id, message_body, message_title, notification_sender, 
                                  schedule_expression, next_time=None, date=None, expiration_date=None):
        update_query = '''
            UPDATE notifications
            SET message_body = $1, message_title = $2, notification_sender = $3, 
                schedule_expression = $4, next_time = $5, date = $6, expiration_date = $7
            WHERE id = $8;
        '''
        async with self.pool.acquire() as connection:
            await connection.execute(update_query, 
                message_body, message_title, notification_sender, schedule_expression,
                next_time, date, expiration_date, notification_id
            )
    
    async def update_notification_next_time(self, notification_id, next_time):
        update_query = '''
            UPDATE notifications
            SET next_time = $1
            WHERE id = $2;
        '''
        async with self.pool.acquire() as connection:
            await connection.execute(update_query, next_time, notification_id)

    async def delete_notification(self, notification_id):
        delete_query = '''
            DELETE FROM notifications
            WHERE id = $1;
        '''
        async with self.pool.acquire() as connection:
            await connection.execute(delete_query, notification_id)

    async def delete_all_notifications(self):
        delete_query = '''
            DELETE FROM notifications;
        '''
        async with self.pool.acquire() as connection:
            await connection.execute(delete_query)

    async def get_all_notifications(self):
        select_query = '''
            SELECT * FROM notifications;
        '''
        async with self.pool.acquire() as connection:
            return await connection.fetch(select_query)
        
    async def get_all_notifications_batch(self, batch_size: int = 10):
        offset = 0
        async with self.pool.acquire() as connection:
            while True:
                query = "SELECT * FROM notifications LIMIT $1 OFFSET $2"
                batch = await connection.fetch(query, batch_size, offset)
                if not batch:
                    break
                yield batch
                offset += batch_size

async def main():
    db_handler = NotificationDBHandler(
        dbname='postgres',
        user='postgres',
        password='postgres',
        host='localhost',
        port='5432'
    )

    await db_handler.create_pool()


    await db_handler.create_notification_table()
    import uuid
    for _ in range(10):
        print('insertando')
        notification_id = uuid.uuid4()
        user_id = uuid.uuid4()
        random_number_from_1_to_10 = str(random.randint(1, 10))
        notification_id = await db_handler.insert_notification(
            notification_id=notification_id,
            message_body='test',
            message_title=f'cada {random_number_from_1_to_10} min',
            notification_sender='test2',
            schedule_expression=f"*/{random_number_from_1_to_10} * * * *",
            user_id=user_id
        )


    notifications = await db_handler.get_all_notifications()
    print(notifications)

    # Actualizar una notificación con columnas opcionales
    # await db_handler.update_notification(
    #     notification_id=notification_id,
    #     message_body='nuevo cuerpo',
    #     message_title='nuevo título',
    #     notification_sender='nuevo remitente',
    #     schedule_expression='*/10 * * * *',
    #     next_time=datetime.strptime('2024-01-12 12:00:00', '%Y-%m-%d %H:%M:%S'),
    #     date=datetime.strptime( '2024-01-12 12:30:00', '%Y-%m-%d %H:%M:%S'),
    #     expiration_date=datetime.strptime('2024-01-12 13:00:00', '%Y-%m-%d %H:%M:%S')
    # )

    # Eliminar una notificación
    # await db_handler.delete_notification(notification_id)
    # await db_handler.delete_all_notifications()
    # notifications = await db_handler.get_notifications(user_id=1)
    # print(notifications)
    # Cerrar la conexión
    await db_handler.close_pool()

# Ejecutar el bucle de eventos de asyncio
if __name__ == "__main__":
    asyncio.run(main())
