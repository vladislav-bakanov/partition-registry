from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

from partition_registry.orm import PartitionsRegistryORM
from partition_registry.orm import ProvidersRegistryORM
from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.registry import Registry

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.partition import SimplePartition

from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist


class PartitionRegistry(Registry[SimplePartition, RegisteredPartition]):
    def __init__(self, session: Session) -> None:
        self.session = session
        self.table = PartitionsRegistryORM
        self.providers_table = ProvidersRegistryORM
        self.sources_table = SourcesRegistryORM
        self.cache: dict[tuple[SimplePartition, RegisteredSource, RegisteredProvider], RegisteredPartition] = dict()

    def safe_register(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartition:
        match self.lookup_registered(partition, source, provider):
            case RegisteredPartition() as registered_partition:
                print("Cache hit...")
                return registered_partition

        print("Cache miss...")
        registered_partition = RegisteredPartition(
            start=partition.start,
            end=partition.end,
            source=source,
            provider=provider,
            created_at=partition.created_at
        )
        match self.persist(registered_partition, source, provider):
            case SuccededPersist(): ...
            case fail:
                # TODO: create specific error for this
                raise ValueError(fail.error_message)
        key = (partition, source, provider)
        self.cache[key] = registered_partition
        return registered_partition

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
            .join(self.sources_table, self.table.source_id == self.sources_table.id)
            .join(self.providers_table, self.table.provider_id == self.providers_table.id)
            .filter(self.sources_table.name == source.name)
            .filter(self.providers_table.name == provider.name)
            .filter(self.table.start == partition.start)
            .filter(self.table.end == partition.end)
            .all()
        )
        if not rows:
            return None

        for row in rows:
            return RegisteredPartition(row.start, row.end, source, provider, row.created_at, row.registered_at)

    def persist(
        self,
        partition: RegisteredPartition,
        source: RegisteredSource,
        provider: RegisteredProvider,
    ) -> SuccededPersist | FailedPersist:
        session = self.session
        source_id = (
            session
            .query(self.sources_table.id)
            .filter(self.sources_table.name == source.name)
            .one_or_none()
        )
        if not source_id:
            return FailedPersist(f"Persist failed. Source <<{source.name}>> not found among registered sources. Please, be sure your source registered...")
        
        provider_id = (
            session
            .query(self.providers_table.id)
            .filter(self.providers_table.name == provider.name)
            .one_or_none()
        )
        if not provider_id:
            return FailedPersist(f"Persist failed. Provider <<{provider.name}>> not found among registered provider. Please, be sure your provider registered...")

        record = PartitionsRegistryORM(
            start=partition.start,
            end=partition.end,
            source_id=source_id[0],
            provider_id=provider_id[0],
            created_at=partition.created_at
        )
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
        return SuccededPersist()