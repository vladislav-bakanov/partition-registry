from typing import Protocol
from typing import TypeVar

from partition_registry.data.status import Status

T_contra = TypeVar('T_contra', contravariant=True)
T_status = TypeVar('T_status', bound=Status, covariant=True)

class Registry(Protocol[T_contra, T_status]):
    def register(self, obj: T_contra) -> T_status: ...
    def persist(self) -> T_status: ...