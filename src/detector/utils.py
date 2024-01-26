from functools import wraps
from asyncio import sleep
from datetime import datetime, timedelta

def loopwait(seconds: int):
    def decorator_repeat(func):
        @wraps(func)
        async def wrapper_repeat(*args, **kwargs):
            now = datetime.now()
            seconds_until_next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=1) - now).total_seconds()

            await sleep(seconds_until_next_minute)

            while True:
                await func(*args, **kwargs)
                await sleep(seconds)
        return wrapper_repeat
    return decorator_repeat