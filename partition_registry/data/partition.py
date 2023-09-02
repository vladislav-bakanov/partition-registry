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


@dc.dataclass
class DesiredPartition(Partition):
    ...


@dc.dataclass
class ReadyPartition(Partition):
    @property
    def is_ready(self):
        return True


@dc.dataclass
class NotReadyPartition(Partition):
    @property
    def is_ready(self):
        return False
