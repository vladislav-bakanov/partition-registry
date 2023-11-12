from collections import defaultdict
import datetime as dt
import pytz

from partition_registry.data.registry import Registry
from partition_registry.actor.registry.provider.orm import ProviderRegistryORM

from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import Provider

from partition_registry.data.source import RegisteredSource
from partition_registry.data.status import Status
from partition_registry.data.status import EXIST
from partition_registry.data.status import NOT_EXIST

from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist


class ProviderRegistry(Registry[Provider, Status]):
    def __init__(
        self,
        table: type[ProviderRegistryORM],
    ) -> None:
        self.table = table
        self._registry: set[Provider] = set()

    def register(self, obj: Provider) -> SuccededRegistration | FailedRegistration:
        match self.is_registered(obj):
            case NOT_EXIST():
                return FailedRegistration(f"Provider \"{obj}\" already registered...")
            case EXIST():
                try:
                    self._registry.add(obj)
                except Exception as e:
                    return FailedRegistration(f"Registration failed with error: {e}")
                return SuccededRegistration()
            case s:
                raise ValueError(f"Expected statuses EXIST | NOT_EXIST, got {s}")

    def persist(self) -> SuccededPersist | FailedPersist:
        return FailedPersist("DONT FORGET ME")

    @property
    def registry(self) -> set[Provider]:
        return self._registry

    def is_registered(self, obj: Provider) -> EXIST | NOT_EXIST:
        if obj not in self._registry:
            return NOT_EXIST()
        return EXIST()