import dataclasses as dc

from partition_registry.data.provider_type import ProviderType
from partition_registry.data.exceptions import IncorrectProviderNameError


@dc.dataclass(frozen=True)
class Provider:
    name: str
    provider_type: ProviderType

    def validate(self) -> None:
        if not self.name or not self.name.strip():
            raise IncorrectProviderNameError("Provider's name can't be empty...")
