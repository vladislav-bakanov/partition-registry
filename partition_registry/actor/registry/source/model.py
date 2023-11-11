from partition_registry.data.registry import Registry
from partition_registry.actor.registry.source.orm import SourceRegistryORM

from partition_registry.data.source import Source
from partition_registry.data.status import Status

from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist


class SourceRegistry(Registry[Source, Status]):
    def __init__(
        self,
        table: type[SourceRegistryORM]
    ) -> None:
        self.table = table
        self._registry: set[Source] = set()
    
    def register(self, obj: Source) -> SuccededRegistration | FailedRegistration:
        try:
            self._registry.add(obj)
        except Exception as e:
            return FailedRegistration(f"Registration failed with error: {e}")
        return SuccededRegistration()

    def persist(self) -> SuccededPersist | FailedPersist:
        return FailedPersist("DONT FORGET ME")
