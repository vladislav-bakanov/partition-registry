from typing import Protocol
import dataclasses as dc
from partition_registry.data.access_token import AccessToken

class Provider(Protocol):
    name: str

    def __str__(self) -> str: ...
    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class SimpleProvider(Provider):
    name: str

    def __str__(self) -> str:
        return f"SimpleProvider(name={self.name})"

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class RegisteredProvider(Provider):
    name: str
    access_token: AccessToken

    def __str__(self) -> str:
        return f"RegisteredProvider(name={self.name}, access_token={self.access_token})"