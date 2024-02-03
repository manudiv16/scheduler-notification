from uuid import UUID
from returns.result import Result
from returns.future import FutureResult, Future
from typing import Any, AsyncIterator,Iterator, List
from abc import ABC, abstractmethod

class Repository[T](ABC): # type: ignore
    
    @abstractmethod
    def add(self, object: T) -> FutureResult[UUID, Any]:
        raise NotImplementedError

    @abstractmethod
    def get(self, id: UUID) -> FutureResult[T, Any]:
        raise NotImplementedError

    @abstractmethod
    def get_all(self) -> AsyncIterator[List[Result[T, Any]]]:
        raise NotImplementedError


    @abstractmethod
    async def update(self, id: UUID, **kwargs: object) -> None:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, id: UUID) -> None:
        raise NotImplementedError
    
    @abstractmethod
    async def delete_all(self) -> None:
        raise NotImplementedError
    
