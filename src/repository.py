from uuid import UUID
from returns.result import Result
from abc import ABC, abstractmethod
from returns.future import FutureResult
from typing import Any, AsyncIterator, List

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
    def update(self, id: UUID, **kwargs: object) -> FutureResult[UUID, Any]:
        raise NotImplementedError

    @abstractmethod
    def delete(self, id: UUID) -> FutureResult[UUID, Any]:
        raise NotImplementedError
    
    @abstractmethod
    def delete_all(self) -> FutureResult[str, Any]:
        raise NotImplementedError
    
