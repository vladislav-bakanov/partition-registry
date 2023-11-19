from typing import Protocol
import dataclasses as dc
import datetime as dt
from partition_registry.data.access_token import AccessToken


class Source(Protocol):
    name: str

    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class SimpleSource(Source):
    name: str

    def __str__(self) -> str:
        return f"SimpleSource(name={self.name})"


@dc.dataclass(frozen=True)
class RegisteredSource(Source):
    name: str
    registered_at: dt.datetime
    access_token: AccessToken

    def __str__(self) -> str:
        return f"RegisteredSource(name={self.name}, registered_at={self.registered_at}, access_token={self.access_token})"
