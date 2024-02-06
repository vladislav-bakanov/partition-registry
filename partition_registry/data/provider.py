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
        return f"{self.__class__.__name__}(name={self.name})"

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class RegisteredProvider(Provider):
    name: str
    access_token: AccessToken = dc.field(repr=False)
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"name={self.name}, "
            f"registered_at={self.registered_at}"
            ")"
        )
