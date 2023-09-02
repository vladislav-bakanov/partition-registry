import dataclasses as dc
import datetime as dt

from typing import Union

# from partition_registry.data.source import BigQuerySource
# from partition_registry.data.source import PostgreSQLSource
# from partition_registry.data.source import AirflowDAGSource

from partition_registry.data.partition_size import PartitionSize

from partition_registry.data.exceptions import NotPositiveIntervalError


@dc.dataclass
class Partition:
    # source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource]
    startpoint: dt.datetime
    endpoint: dt.datetime

    def validate(self) -> None:
        """Validate partition.

        Raises:
            NotPositiveIntervalError: in case if partition initialized as not positive interval (<0).
        """
        if self.startpoint >= self.endpoint:
            raise NotPositiveIntervalError(f"Partition \"{self}\" represented as negative interval")

    def __hash__(self) -> int:
        return hash((self.startpoint, self.endpoint))

    @property
    def size_in_sec(self) -> float:
        """Interval size represented as number of seconds in interval"""
        return (self.endpoint - self.startpoint).total_seconds()
