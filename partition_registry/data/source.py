import dataclasses as dc


@dc.dataclass(frozen=True)
class Source:
    name: str

    def __str__(self) -> str:
        return f"Source(name={self.name})"

    def __repr__(self) -> str:
        return self.__str__()
