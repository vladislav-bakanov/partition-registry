import dataclasses as dc
import datetime as dt

from partition_registry.data.exceptions import NotPositiveIntervalError

@dc.dataclass
class Partition:
    startpoint: dt.datetime
    endpoint: dt.datetime

    def validate(self) -> None:
        """Validate partition.

        Raises:
            NotPositiveIntervalError: in case if partition initialized as not positive interval (<0).
        """
        if self.startpoint >= self.endpoint:
            raise NotPositiveIntervalError(f"Partition \"{self}\" represented as negative interval")

    @property
    def size_in_sec(self) -> int:
        """Interval size represented as number of seconds in interval"""
        return int((self.endpoint - self.startpoint).total_seconds())


@dc.dataclass
class DesiredPartition(Partition):
    ...


@dc.dataclass
class SourcePartition(Partition):
    is_ready: bool
