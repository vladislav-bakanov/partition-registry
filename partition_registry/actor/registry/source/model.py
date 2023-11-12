import datetime as dt
import pytz

from collections import defaultdict

from partition_registry.data.registry import Registry
from partition_registry.actor.registry.source.orm import SourceRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource
from partition_registry.data.status import Status
from partition_registry.data.status import EXIST
from partition_registry.data.status import NOT_EXIST

from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist


class SourceRegistry(Registry[RegisteredSource, Status]):
    def __init__(
        self,
        table: type[SourceRegistryORM]
    ) -> None:
        self.table = table
        self._registry: dict[SimpleSource, RegisteredSource] = self.load_registered_from_database() or defaultdict(str)
    
    def load_registered_from_database(self) -> dict[SimpleSource, RegisteredSource]:
        return defaultdict(str)

    def register(self, obj: SimpleSource) -> SuccededRegistration | FailedRegistration:
        match self.is_registered(obj):
            case EXIST():
                return FailedRegistration(f"Source \"{obj}\" already registered...")
            case NOT_EXIST():
                try:
                    self._registry[obj] = RegisteredSource(obj.name, dt.datetime.now(pytz.UTC))
                except Exception as e:
                    return FailedRegistration(f"Registration failed with error: {e}")
                return SuccededRegistration()
            case s:
                raise ValueError(f"Expected statuses EXIST | NOT_EXIST, but got {s}")

    def persist(self) -> SuccededPersist | FailedPersist:
        return FailedPersist("DONT FORGET ME")

    @property
    def registry(self) -> dict[SimpleSource, RegisteredSource]:
        return self._registry

    def is_registered(self, obj: SimpleSource) -> EXIST | NOT_EXIST:
        if obj not in self._registry:
            return NOT_EXIST()
        return EXIST()
    
    def get_registered_source(self, obj: SimpleSource) -> RegisteredSource | NOT_EXIST:
        try:
            return self._registry[obj]
        except KeyError as _:
            return NOT_EXIST()