import dataclasses as dc
import datetime as dt
import enum
import pytz


from partition_registry.data.partition import Partition
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider


class EventState(enum.Enum):
    LOCK = enum.auto()
    UNLOCK = enum.auto()


@dc.dataclass(frozen=True)
class PartitionEvent:
    partition: Partition
    source: RegisteredSource
    provider: RegisteredProvider
    state: EventState
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))


@dc.dataclass(frozen=True)    
class RegisteredPartitionEvent(PartitionEvent):
    state: EventState
    created_at: dt.datetime
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
