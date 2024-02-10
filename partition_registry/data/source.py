from typing import Protocol

import datetime as dt
import dataclasses as dc

import pytz

from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import ValidationSucceded
from partition_registry.data.status import ValidationFailed


class Source(Protocol):
    name: str
    owner: str

    def safe_validate(self) -> ValidationSucceded | ValidationFailed:
        if not self.name:
            return ValidationFailed("Source.name shouldn't be empty...")

        for char in self.name:
            if not char.strip():
                return ValidationFailed("Source.name can't contain any spaces...")

        if not self.name:
            return ValidationFailed("Source.owner shouldn't be empty...")

        for char in self.owner:
            if not char.strip():
                return ValidationFailed("Source.owner can't contain any spaces...")

        return ValidationSucceded()

    def __str__(self) -> str: ...

    def __repr__(self) -> str:
        return str(self)


@dc.dataclass(frozen=True)
class SimpleSource(Source):
    name: str
    owner: str

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}"
            f"(name='{self.name}', "
            f"(owner='{self.owner}'"
            ")"
        )


@dc.dataclass(frozen=True)
class RegisteredSource(Source):
    source_id: int
    name: str
    owner: str
    access_token: AccessToken = dc.field(repr=False)
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"source_id={self.source_id}, "
            f"name={self.name}, "
            f"owner={self.owner}"
            f"registered_at={self.registered_at}"
            ")"
        )
