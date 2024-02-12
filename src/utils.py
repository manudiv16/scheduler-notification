from asyncio import sleep
from functools import wraps
from typing import Callable, Any
from dataclasses import dataclass
from typing import Callable, Optional
from datetime import datetime, timedelta

def loopwait(minutes: int) -> Callable[..., Any]:
    def decorator_repeat(func: Callable[..., Any]) -> Callable[..., Any]:
        @wraps(func)
        async def wrapper_repeat(*args: Any, **kwargs: Any) -> Any:
            while True:
                now = datetime.now()
                seconds_until_next_minute = (now.replace(second=0, microsecond=0) + timedelta(minutes=minutes) - now).total_seconds()

                await sleep(seconds_until_next_minute)
                await func(*args, **kwargs)
        return wrapper_repeat
    return decorator_repeat

@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    db: int = 0
    password: Optional[str] = None

__all__ = ['loopwait']