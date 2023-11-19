from typing import Protocol
from typing import TypeVar

from partition_registry.data.status import Status

T_contra = TypeVar('T_contra', contravariant=True)
T_co = TypeVar('T_co', covariant=True)

class Registry(Protocol[T_contra, T_co]):
    def register(self, obj: T_contra) -> T_co: ...
    def is_registered(self, obj: T_contra) -> bool: ...