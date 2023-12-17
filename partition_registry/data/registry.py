from typing import Protocol
from typing import TypeVar


T_contra = TypeVar('T_contra', contravariant=True)
T_co = TypeVar('T_co', covariant=True)

class Registry(Protocol[T_contra, T_co]):
    REDIS_REGISTRY_TAG = 'registry'

    def safe_register(self, obj: T_contra) -> T_co:
        """Function to register object within the registry"""
