from uuid import UUID
from returns.result import Result
from typing import Any, AsyncGenerator, List
from abc import ABC, abstractmethod

class Repository[T](ABC): 

    @abstractmethod
    async def create(self) -> 'Repository[T]':
        raise NotImplementedError
    
    @abstractmethod
    async def add(self, **kwargs: T) -> Result[UUID, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get(self, id: UUID) -> Result[T, Any]:
        raise NotImplementedError

    @abstractmethod
    async def get_all(self) -> AsyncGenerator[List[Result[T, Any]], None]:
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
    
