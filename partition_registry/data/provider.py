import dataclasses as dc
import datetime as dt
import pytz

from typing import Protocol

from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import ValidationSucceded


class Provider(Protocol):
    name: str
    
    def safe_validate(self) -> ValidationFailed | ValidationSucceded:
        # TODO: implement
        ...
        
    
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
    provider_id: int
    name: str
    access_token: AccessToken = dc.field(repr=False)
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"provider_id={self.provider_id}, "
            f"name={self.name}, "
            f"registered_at={self.registered_at}"
            ")"
        )
