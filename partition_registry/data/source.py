import datetime as dt
import dataclasses as dc
from typing import Protocol
from typing import Self

import pytz

from partition_registry.data.access_token import AccessToken


class Source(Protocol):
    name: str

    def validate(self) -> None:
        """
        Validate source.
        Expected, that:
        - Source should have a non-empty name.
            Raises: ValueError()
        """
        if not self.name:
            raise ValueError("Source name shouldn't be empty...")
        for char in self.name:
            if not char.strip():
                raise ValueError("Source name shouldn't contain any spaces...")

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
    access_token: AccessToken
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return f"RegisteredSource(name={self.name}, access_token={self.access_token}), registered_at={self.registered_at}"
