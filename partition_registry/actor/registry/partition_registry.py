import datetime as dt

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

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
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import AccessDenied

from partition_registry.data.func import localize


class PartitionRegistry:
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = PartitionsRegistryORM
        self.cache: dict[tuple[dt.datetime, dt.datetime, RegisteredSource, RegisteredProvider], RegisteredPartition] = dict()

    def safe_register(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source_name: str,
        source_registry: SourceRegistry,
        provider_name: str,
        provider_registry: ProviderRegistry,
    ) -> RegisteredPartition | FailedPersist | AlreadyRegistered | ValidationFailed | LookupFailed | AccessDenied:
        simple_partition = SimplePartition(localize(start), localize(end))
        match simple_partition.safe_validate():
            case ValidationFailed() as failed_validation:
                return failed_validation
        
        match source_registry.lookup_registered(source_name):
            case RegisteredSource() as registered_source: ...
            case LookupFailed() as lookup_failed:
                return lookup_failed

        match provider_registry.lookup_registered(provider_name):
            case RegisteredProvider() as registered_provider: ...
            case LookupFailed() as lookup_failed:
                return lookup_failed
        
        if registered_provider.access_token != registered_source.access_token:
            return AccessDenied(
                f"<<{registered_provider}>> has no access to source <<{registered_source.name}>>. "
                f"Ask <<{registered_source.owner}>> to get access to the source..."
            )

        match self.lookup_registered(
            simple_partition.start,
            simple_partition.end,
            registered_source,
            registered_provider
        ):
            case RegisteredPartition() as registered_partition:
                return AlreadyRegistered(simple_partition)

        match self.persist(start, end, registered_source, registered_provider):
            case RegisteredPartition() as registered_partition:
                key = (start, end, registered_source, registered_provider)
                self.cache[key] = registered_partition
                return registered_partition
            case FailedPersist() as failed_persist:
                return failed_persist

    def lookup_registered(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition | LookupFailed:
        start = localize(start)
        end = localize(end)
        return self.memory_lookup(start, end, source, provider) or self.db_lookup(start, end, source, provider)
    
    def memory_lookup(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source: RegisteredSource,
        provider: RegisteredProvider,
    ) -> RegisteredPartition | LookupFailed:
        key = (localize(start), localize(end), source, provider)
        return self.cache.get(
            key,
            LookupFailed(
                f"Partition <<{start}:{end}>> for Source<<{source.name}>> and "
                f"Provider<<{provider.name}>> not found..."
            )
        )
    
    def is_registered(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> bool:
        return isinstance(self.lookup_registered(start, end, source, provider), RegisteredPartition)
    
    def db_lookup(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition | LookupFailed:
        session = self.session
        rows = (
            session
            .query(self.table)
            .join(SourcesRegistryORM, self.table.source_id == SourcesRegistryORM.id)
            .join(ProvidersRegistryORM, self.table.provider_id == ProvidersRegistryORM.id)
            .filter(SourcesRegistryORM.name == source.name)
            .filter(ProvidersRegistryORM.name == provider.name)
            .filter(self.table.start == start)
            .filter(self.table.end == end)
            .all()
        )

        for row in rows:
            return RegisteredPartition(
                partition_id=row.id,
                start=row.start,
                end=row.end,
                source=source,
                provider=provider,
                registered_at=row.registered_at
            )
        return LookupFailed(
            f"Partition <<{start}:{end}>> for Source<<{source.name}>> and "
            f"Provider<<{provider.name}>> not found..."
        )

    def persist(
        self,
        start: dt.datetime,
        end: dt.datetime,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition | FailedPersist:
        session = self.session
        record = PartitionsRegistryORM(
            start=localize(start),
            end=localize(end),
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
