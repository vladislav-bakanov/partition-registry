import dataclasses as dc


@dc.dataclass
class PartitionSize:
    size_in_sec: float

    def __hash__(self) -> float:
        return hash(self.size_in_sec)
