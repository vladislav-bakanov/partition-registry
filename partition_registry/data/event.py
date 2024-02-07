import dataclasses as dc
import datetime as dt
import enum
import pytz


from partition_registry.data.partition import RegisteredPartition
from partition_registry.data.partition import SimplePartition
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider


class EventType(enum.Enum):
    LOCK = 'LOCK'
    UNLOCK = 'UNLOCK'

@dc.dataclass(frozen=True)
class SimplifiedPartitionEventORM:
    id: int
    event_type: EventType
    registered_at: dt.datetime


@dc.dataclass(frozen=True)
class PartitionEvent:
    partition: SimplePartition
    source: RegisteredSource
    provider: RegisteredProvider
    event_type: EventType
    created_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))


@dc.dataclass(frozen=True)    
class RegisteredPartitionEvent(PartitionEvent):
    partition: RegisteredPartition
    source: RegisteredSource
    provider: RegisteredProvider
    event_type: EventType
    created_at: dt.datetime
    registered_at: dt.datetime = dc.field(default=dt.datetime.now(pytz.UTC))
