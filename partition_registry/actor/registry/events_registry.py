import datetime as dt

from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import and_

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.orm import PartitionEventsORM
from partition_registry.orm import PartitionsRegistryORM
from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.status import FailedPersist
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import LookupFailed

from partition_registry.data.event import SimplePartitionEvent
from partition_registry.data.event import SimplifiedPartitionEventORM
from partition_registry.data.event import RegisteredPartitionEvent
from partition_registry.data.event import EventType


class EventsRegistry:
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = PartitionEventsORM
        self.cache: dict[SimplePartitionEvent, RegisteredPartitionEvent] = dict()

    def safe_register(
        self,
        start: dt.datetime,
        end: dt.datetime,
        partition_registry: PartitionRegistry,
        source_name: str,
        source_registry: SourceRegistry,
        provider_name: str,
        provider_registry: ProviderRegistry,
        event_type: EventType
    ) -> RegisteredPartitionEvent | ValidationFailed | FailedPersist | LookupFailed:
        
        simple_partition = SimplePartition(start, end)
        match simple_partition.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation

        match partition_registry.lookup_registered(simple_partition, source, provider):
            case None:
                return LookupFailed(f"<<{partition}>> by [<<{source}>>, <<{provider}>>] is not registered. Register partition first...")
            case RegisteredPartition() as registered_partition: ...

        event = SimplePartitionEvent(
            partition=partition,
            source=source,
            provider=provider,
            event_type=event_type
        )

        match self.persist(registered_partition, event):
            case FailedPersist() as failed_persist:
                return failed_persist
            case RegisteredPartitionEvent() as registered_event:
                self.cache[event] = registered_event
                return registered_event

    def persist(
        self,
        partition: RegisteredPartition,
        event: SimplePartitionEvent
    ) -> RegisteredPartitionEvent | FailedPersist:
        session = self.session    
        record = PartitionEventsORM(
            partition_id=partition.partition_id,
            event_type=event.event_type.value,
        )
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
        
        match record.event_type:
            case EventType.LOCK.value:
                event_type = EventType.LOCK
            case EventType.UNLOCK.value:
                event_type = EventType.UNLOCK
            case unexpected:
                return FailedPersist(f"Event type has unexpected value: {unexpected}")
        
        return RegisteredPartitionEvent(
            partition=partition,
            event_type=event_type,
            registered_at=record.registered_at
        )

    def get_source_partitions(
        self,
        partition: SimplePartition,
        source: RegisteredSource
    ) -> list[PartitionsRegistryORM]:
        """Get all events by source and provider(optionally), where specified ts is within the partitions
        """

        rows = (
            self.session
            .query(PartitionsRegistryORM)
            .join(SourcesRegistryORM, SourcesRegistryORM.id == PartitionsRegistryORM.source_id)
            .filter(SourcesRegistryORM.name == source.name)
            .filter(
                or_(
                    and_(
                        PartitionsRegistryORM.start <= partition.start,
                        partition.start < PartitionsRegistryORM.end
                    ),
                    and_(
                        PartitionsRegistryORM.start < partition.end,
                        partition.end <= PartitionsRegistryORM.end
                    )
                )
            )
            .distinct()
            .all()
        )
        return rows

    
    def get_last_partition_events(
        self,
        partitions: list[PartitionsRegistryORM]
    ) -> list[SimplifiedPartitionEventORM]:
        last_events_subquery = (
            self.session
            .query(
                self.table.partition_id,
                func.max(self.table.registered_at)
                    .over(partition_by=self.table.partition_id)
                    .label('max_registered_at')
            )
            .filter(self.table.partition_id.in_([partition.id for partition in partitions]))
            .subquery('last_events')
        )

        rows = (
            self.session
            .query(self.table.partition_id, self.table.event_type, self.table.registered_at)
            .join(
                last_events_subquery, 
                and_(
                    last_events_subquery.c.partition_id == self.table.partition_id,
                    last_events_subquery.c.max_registered_at == self.table.registered_at
                )
            )
            .distinct()
            .all()
        )
        
        return [SimplifiedPartitionEventORM(row[0], row[1], row[2]) for row in rows]
