from typing import Protocol
from typing import TypeVar
from partition_registry.data.response import BaseResponse
from partition_registry.data.source import Source
from partition_registry.data.provider import Provider


T = TypeVar('T')

class Registry(Protocol[T]):
    def register(self, obj: T) -> BaseResponse:
        ...


class SourceRegistry(Registry):
    def register(self, obj: Source) -> BaseResponse:
        ...


class ProviderRegistry(Registry):
    def register(self, obj: Provider) -> BaseResponse:
        ...
