from enum import Enum
from enum import auto


class PartitionStrategy(Enum):
    PARTITIONED = auto()
    NOT_PARTITIONED = auto()
