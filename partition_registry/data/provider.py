import dataclasses as dc

from partition_registry.data.exceptions import IncorrectProviderNameError


@dc.dataclass(frozen=True)
class Provider:
    name: str

    def __post_init__(self) -> None:
        if not self.name.strip():
            raise IncorrectProviderNameError("Provider name is empty")
