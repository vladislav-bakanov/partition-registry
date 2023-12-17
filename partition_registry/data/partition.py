import dataclasses as dc
import datetime as dt

from typing import Protocol
from typing import Self

from functools import cached_property

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

    def __hash__(self) -> int:
        return hash(str(self.start) + str(self.end))


@dc.dataclass(frozen=True)
class SimplePartition(Partition):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return "SimplePartition(\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n" \
        ")"

    def __hash__(self) -> int:
        return hash(str(self.start) + str(self.end) + str(self.created_at))


@dc.dataclass(frozen=True)
class LockedPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    locked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return "LockedPartition(\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n  " \
            f"locked_at='{self.locked_at}',\n" \
        ")"
    
    @classmethod
    def parse(cls, obj: SimplePartition) -> Self:
        return LockedPartition(obj.start, obj.end, obj.created_at)


@dc.dataclass(frozen=True)
class UnlockedPartition(Partition):
    start: dt.datetime
    end: dt.datetime
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    locked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
    unlocked_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))

    def __str__(self) -> str:
        return "UnlockedPartition(\n  " \
            f"start='{self.start}',\n  " \
            f"end='{self.end}',\n  " \
            f"created_at='{self.created_at}',\n  " \
            f"locked_at='{self.locked_at}',\n  " \
            f"unlocked_at='{self.unlocked_at}'\n" \
        ")"
    
    @classmethod
    def parse(cls, obj: LockedPartition) -> Self:
        return UnlockedPartition(obj.start, obj.end, obj.created_at, obj.locked_at)


def is_intersected(p1: "Partition", p2: "Partition") -> bool:
    """
    Check that 2 partitions intersect.
    The order of partitions to check doesn't matter (see ./tests/tests_intersections for more details)
    p1 (Partition) - partition #1 to check
    p2 (Partition) - partition #2 to check
    """
    return (
        p1.start <= p2.start < p1.end or p1.start < p2.end <= p1.end
        or
        p2.start <= p1.start < p2.end or p2.start < p1.end <= p2.end
    )
