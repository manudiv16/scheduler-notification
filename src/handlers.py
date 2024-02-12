from typing import Any
from returns.io import IOResult
from returns.result import Result
from dataclasses import dataclass
from abc import ABC, abstractmethod

@dataclass
class Handler[T](ABC):
    @abstractmethod
    async def handle(self, object: Result[T,Any]) -> IOResult[Any, Any]: 
        pass
