import dataclasses as dc

from partition_registry.data.provider_type import ProviderType


@dc.dataclass(frozen=True)
class Provider:
    name: str
    provider_type: ProviderType
