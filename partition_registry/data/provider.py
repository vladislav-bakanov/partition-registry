import dataclasses as dc
from partition_registry.data.source import RegisteredSource


@dc.dataclass(frozen=True)
class Provider:
    name: str
    source: RegisteredSource

    def __str__(self) -> str:
        return f"Provider(name={self.name}, source={self.source})"

    def __repr__(self) -> str:
        return self.__str__()
