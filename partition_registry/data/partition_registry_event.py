import dataclasses as dc
import datetime as dt
from typing import Union

from partition_registry.data.partition import ReadyPartition
from partition_registry.data.partition import NotReadyPartition

from partition_registry.data.exceptions import DifferentEventsWithTheSameTimestampError
from partition_registry.data.exceptions import UnknownPartitionTypeError


@dc.dataclass
class PartitionRegistryEvent:
    partition: Union[ReadyPartition, NotReadyPartition]
    created_date: dt.datetime

    def __str__(self) -> str:
        return f"""Source event:
    Interval: [{self.partition.startpoint} : {self.partition.endpoint}]
       State: | {'  READY  ' if self.partition.is_ready else 'NOT_READY'} |
     Created: | {self.created_date} |
    """

    def __hash__(self) -> int:
        return hash((self.partition.startpoint, self.partition.endpoint, self.partition.is_ready, self.created_date))

    def validate(self) -> None:
        """Validate Partition Registry event. Also validates partition. 

        Raises:
            DifferentEventsWithTheSameTimestampError: in case if event was created earlier or exactly at the time of partition ended.
        """
        self.partition.validate()
        if self.created_date <= self.partition.endpoint:
            raise DifferentEventsWithTheSameTimestampError("Event shouldn't be created earlier or exactly at the time of partition ended")
