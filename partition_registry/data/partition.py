import dataclasses as dc
import datetime as dt

from typing import Protocol
from typing import Self

from functools import cached_property

from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.source import RegisteredSource

import pytz


class Partition(Protocol):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime

    @cached_property
    def size(self) -> float:
        """Partition size in seconds"""
        return (self.end - self.start).total_seconds()

    def validate(self) -> None:
        """Validate partition properties
        Raises:
            ValueError(): in case if partition has the same start and the end
            ValueError(): in case if partition has end erlier than the end
        """
        if self.start == self.end:
            raise ValueError("Partition start and end should be different")
        if self.size < 0:
            raise ValueError("Partition start should be earlier than partition end")

    def __str__(self) -> str:
        ...

    def __repr__(self) -> str:
        return self.__str__()


@dc.dataclass(frozen=True)
class SimplePartition(Partition):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}', "
            f"created_at='{self.created_at}'"
            ")"
        )


@dc.dataclass(frozen=True)
class RegisteredPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    source: RegisteredSource
    provider: RegisteredProvider
    created_at: dt.datetime
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}', "
            f"source='{self.source}', "
            f"provider='{self.provider}', "
            f"created_at='{self.created_at}', "
            f"registered_at='{self.registered_at}'"
            ")"
        )


@dc.dataclass(frozen=True)
class LockedPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    source: RegisteredSource
    provider: RegisteredProvider
    created_at: dt.datetime
    registered_at: dt.datetime
    locked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}', "
            f"source='{self.source}', "
            f"provider='{self.provider}', "
            f"created_at='{self.created_at}', "
            f"registered_at='{self.registered_at}', "
            f"locked_at='{self.locked_at}'"
            ")"
        )

@dc.dataclass(frozen=True)
class UnlockedPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    source: RegisteredSource
    provider: RegisteredProvider
    created_at: dt.datetime
    registered_at: dt.datetime
    locked_at: dt.datetime
    unlocked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return (
            f"{self.__class__.__name__}("
            f"start='{self.start}', "
            f"end='{self.end}', "
            f"source='{self.source}', "
            f"provider='{self.provider}', "
            f"created_at='{self.created_at}', "
            f"registered_at='{self.registered_at}', "
            f"locked_at='{self.locked_at}', "
            f"unlocked_at='{self.unlocked_at}'"
            ")"
        )


def is_intersected(p1: "Partition", p2: "Partition") -> bool:
    """
    Check that 2 partitions intersect.
    The order of partitions to check doesn't matter (see ./tests/tests_intersections for more details)
    p1 (Partition) - partition #1 to check
    p2 (Partition) - partition #2 to check
    """
    left = p1 if p1.start <= p2.start else p2
    right = p2 if p1.start <= p2.start else p1
    return left.start <= right.start < left.end or left.start < right.end <= left.end
