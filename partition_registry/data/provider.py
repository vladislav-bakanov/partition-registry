import dataclasses as dc
import datetime as dt
import pytz

from typing import Protocol

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
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return f"RegisteredProvider(name={self.name}, access_token={self.access_token}, registered_at={self.registered_at})"
