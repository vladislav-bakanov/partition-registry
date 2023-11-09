import datetime as dt
import pytz

from partition_registry_v2.data.partition import Partition
from partition_registry_v2.data.partition import LockedPartition
from partition_registry_v2.data.partition import UnlockedPartition

from partition_registry_v2.data.source import Source


class Provider:
    name: str
    source: Source

    def __str__(self) -> str:
        return f"Provider(name={self.name}, source={self.source})"

    def __repr__(self) -> str:
        return self.__str__()
