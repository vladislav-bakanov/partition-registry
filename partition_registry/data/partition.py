from typing import Protocol

import dataclasses as dc
import datetime as dt

from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.source import RegisteredSource

from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import ValidationSucceded

import pytz


class Partition(Protocol):
    start: dt.datetime
    end: dt.datetime

    def safe_validate(self) -> ValidationFailed | ValidationSucceded:
        """Validate partition properties
        Raises:
            ValueError(): in case if partition has the same start and the end
            ValueError(): in case if partition has end erlier than the end
        """
        if self.start == self.end:
            return ValidationFailed("Partition start and end should be different")
        if self.start <= self.end:
            return ValidationFailed("Partition start should be earlier than partition end")
        
        return ValidationSucceded()

    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class SimplePartition(Partition):
    start: dt.datetime
    end: dt.datetime

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}'"
            ")"
        )


@dc.dataclass(frozen=True)
class RegisteredPartition(Partition):
    partition_id: int
    start: dt.datetime
    end: dt.datetime
    source: RegisteredSource
    provider: RegisteredProvider
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}', "
            f"source='{self.source}', "
            f"provider='{self.provider}', "
            f"registered_at='{self.registered_at}'"
            ")"
        )
