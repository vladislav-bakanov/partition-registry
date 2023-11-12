import dataclasses as dc
import datetime as dt
import pytz

from functools import cached_property

from typing import Protocol
from partition_registry.data.source import Source
from partition_registry.data.provider import Provider


class Partition(Protocol):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    
    @cached_property
    def size(self) -> float:
        """Partition size in seconds"""
        return (self.end - self.start).total_seconds()
    
    def validate(self) -> None:
        if self.start == self.end:
            raise ValueError("Partition start and end should be different")
        if self.size < 0:
            raise ValueError("Partition start should be earlier than partition end")  # TODO: prepare more explicit error type
        
    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class UnknownPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    
    def __str__(self) -> str:
        return "UnknownPartition(\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n" \
        ")"


@dc.dataclass(frozen=True)
class LockedPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    provider: Provider
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    locked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return "LockedPartition(\n  " \
            f"provider='{self.provider}',\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n  " \
            f"locked_at='{self.locked_at}',\n" \
        ")"


@dc.dataclass(frozen=True)
class UnlockedPartition(LockedPartition):
    start: dt.datetime
    end: dt.datetime
    provider: Provider
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    locked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    unlocked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return "UnlockedPartition(\n  " \
            f"provider='{self.provider}',\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n  " \
            f"locked_at='{self.locked_at}',\n  " \
            f"unlocked_at='{self.unlocked_at}'\n" \
        ")"
