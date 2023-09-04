import dataclasses as dc
import datetime as dt

from typing import Union

from partition_registry.data.partition import Partition
from partition_registry.data.event_state import EventState

from partition_registry.data.provider import Provider

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PostgreSQLSource
from partition_registry.data.source import AirflowDAGSource

from partition_registry.data.exceptions import DifferentEventsWithTheSameTimestampError
from partition_registry.data.exceptions import UnknownEventStateError


@dc.dataclass
class PartitionRegistryEvent:
    source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource]
    provider: Provider
    partition: Partition
    state: EventState
    created_date: dt.datetime

    @property
    def is_ready(self) -> bool:
        if self.state == EventState.READY:
            return True
        elif self.state == EventState.NOT_READY:
            return False
        else:
            raise UnknownEventStateError(
                f"Expected {EventState.READY}|{EventState.NOT_READY} event state, but got {self.state}"
            )

    def validate(self) -> None:
        """Validate Partition Registry event. Also validates partition. 

        Raises:
            DifferentEventsWithTheSameTimestampError:
                in case if event was created earlier or exactly at the time of partition ended.
        """
        self.source.validate()
        self.provider.validate()
        self.partition.validate()
        if self.created_date <= self.partition.endpoint:
            raise DifferentEventsWithTheSameTimestampError(
                "Event shouldn't be created earlier or exactly at the time of partition ended"
            )

    def __str__(self) -> str:
        return f"""Source event:
    Interval: [{self.partition.startpoint} : {self.partition.endpoint}]
       State: | {'  READY  ' if self.is_ready else 'NOT_READY'} |
     Created: | {self.created_date} |
    """
