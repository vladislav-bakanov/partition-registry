from sqlalchemy.orm import Session
from sqlalchemy.orm import scoped_session
from sqlalchemy import func
from sqlalchemy import or_
from sqlalchemy import and_

from partition_registry.actor.registry import PartitionRegistry

from partition_registry.orm import PartitionEventsORM
from partition_registry.orm import PartitionsRegistryORM
from partition_registry.orm import SourcesRegistryORM
from partition_registry.orm import ProvidersRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.partition import Partition

from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import LookupFailed

from partition_registry.data.event import PartitionEvent
from partition_registry.data.event import SimplifiedPartitionEventORM
from partition_registry.data.event import RegisteredPartitionEvent
from partition_registry.data.event import EventType


class EventsRegistry:
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = PartitionEventsORM
        self.cache: dict[PartitionEvent, RegisteredPartitionEvent] = dict()

    def safe_register(
        self,
        partition: SimplePartition,
        partition_registry: PartitionRegistry,
        source: RegisteredSource,
        provider: RegisteredProvider,
        event_type: EventType
    ) -> RegisteredPartitionEvent | ValidationFailed | FailedPersist | LookupFailed:
        match partition.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation
        
        if not partition_registry.is_registered(partition, source, provider):
            return LookupFailed(f"<<{partition}>> by [<<{source}>>, <<{provider}>>] is not registered")
        
        
        event = PartitionEvent(
            partition=partition,
            source=source,
            provider=provider,
            event_type=event_type
        )

        match self.persist(event):
            case SuccededPersist(): ...
            case fail:
                raise ValueError(fail.error_message)
        
        registered_event = RegisteredPartitionEvent(
            partition=event.partition,
            source=event.source,
            provider=event.provider,
            event_type=event.event_type,
            created_at=event.created_at
        )
        self.cache[event] = registered_event
        return registered_event

    def get_partition_id(
        self,
        partition: Partition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> int | None:
        session = self.session
        row = (
            session
            .query(PartitionsRegistryORM.id)
            .join(SourcesRegistryORM, SourcesRegistryORM.id == PartitionsRegistryORM.source_id)
            .join(ProvidersRegistryORM, ProvidersRegistryORM.id == PartitionsRegistryORM.provider_id)
            .filter(ProvidersRegistryORM.name == provider.name)
            .filter(SourcesRegistryORM.name == source.name)
            .filter(PartitionsRegistryORM.start == partition.start)
            .filter(PartitionsRegistryORM.end == partition.end)
            .one_or_none()
        )
        return None if not row else row[0]

    def persist(
        self,
        event: PartitionEvent
    ) -> RegisteredPartitionEvent | FailedPersist:
        session = self.session
        
            
        record = PartitionEventsORM(
            partition_id=partition_id,
            event_type=event.event_type.value,
            created_at=event.created_at
        )
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
        return SuccededPersist()

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
