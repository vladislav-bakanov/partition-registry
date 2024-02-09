from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

from partition_registry.data.registry import Registry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.orm import PartitionsRegistryORM
from partition_registry.orm import ProvidersRegistryORM
from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.partition import SimplePartition

from partition_registry.data.status import FailedPersist
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import LookupFailed
from partition_registry.data.status import AlreadyRegistered


class PartitionRegistry(Registry[SimplePartition, RegisteredPartition]):
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = PartitionsRegistryORM
        self.cache: dict[tuple[SimplePartition, RegisteredSource, RegisteredProvider], RegisteredPartition] = dict()

    def safe_register(
        self,
        partition: SimplePartition,
        source: SimpleSource,
        source_registry: SourceRegistry,
        provider: SimpleProvider,
        provider_registry: ProviderRegistry,
    ) -> RegisteredPartition | FailedPersist | AlreadyRegistered | ValidationFailed | LookupFailed:
        match partition.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation

        match source.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation

        match provider.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation

        match source_registry.lookup_registered(source):
            case RegisteredSource() as registered_source: ...
            case None:
                return LookupFailed(f"<<{source}>> not registered. Register source first...")
        
        match provider_registry.lookup_registered(provider):
            case RegisteredProvider() as registered_provider: ...
            case None:
                return LookupFailed(f"<<{[provider]}>> not registered. Register provider first...")
    
        if self.is_registered(partition, registered_source, registered_provider):
            return AlreadyRegistered(partition)

        match self.persist(partition, registered_source, registered_provider):
            case RegisteredPartition() as registered_partition:
                key = (partition, registered_source, registered_provider)
                self.cache[key] = registered_partition
                return registered_partition
            case FailedPersist() as failed_persist:
                return failed_persist

    def lookup_registered(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition | None:
        return self.memory_lookup(partition, source, provider) or self.db_lookup(partition, source, provider)
    
    def memory_lookup(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider,
    ) -> RegisteredPartition | None:
        key = (partition, source, provider)
        return self.cache.get(key)
    
    def is_registered(self, partition: SimplePartition, source: RegisteredSource, provider: RegisteredProvider) -> bool:
        return isinstance(self.lookup_registered(partition, source, provider), RegisteredPartition)
    
    def db_lookup(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition | None:
        session = self.session
        rows = (
            session
            .query(self.table)
            .join(SourcesRegistryORM, self.table.source_id == SourcesRegistryORM.id)
            .join(ProvidersRegistryORM, self.table.provider_id == ProvidersRegistryORM.id)
            .filter(SourcesRegistryORM.name == source.name)
            .filter(ProvidersRegistryORM.name == provider.name)
            .filter(self.table.start == partition.start)
            .filter(self.table.end == partition.end)
            .all()
        )
        if not rows:
            return None

        for row in rows:
            return RegisteredPartition(
                partition_id=row.id,
                start=row.start,
                end=row.end,
                source=source,
                provider=provider,
                registered_at=row.registered_at
            )

    def persist(self, partition: SimplePartition, source: RegisteredSource, provider: RegisteredProvider) -> RegisteredPartition | FailedPersist:
        session = self.session

        record = PartitionsRegistryORM(
            start=partition.start,
            end=partition.end,
            source_id=source.source_id,
            provider_id=provider.provider_id,
        )
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
            return RegisteredPartition(
                partition_id=record.id,
                start=record.start,
                end=record.end,
                source=source,
                provider=provider,
                registered_at=record.registered_at
            )
