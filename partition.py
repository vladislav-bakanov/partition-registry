import datetime as dt
import dataclasses as dc

from typing import Set
from typing import List

import enum
from functools import lru_cache
from functools import cached_property










class Status(enum.Enum):
    FAILED: enum.auto()
    SUCCEDED: enum.auto()





class Producer:
    name: str
    _type: Producer

    def lock(
        self,
        queue: LockedPartitionsQueue,
        partition: Partition
    ) -> None:
        queue.put(partition)

    def unlock(
        self,
        queue: LockedPartitionsQueue,
        partition: LockedPartition
    ) -> Partition:
        status = queue.persist(partition)
        match status:
            case Status.SUCCEDED:
                queue.remove(partition)
            case Status.FAILED:
                raise ValueError("")


class Consumer:
    @classmethod
    def is_partition_ready(
        queue: LockedPartitionsQueue,
        partition: Partition,
    ) -> bool:
        if queue.is_partition_locked(partition):
            return False
        for partition in queue.get()
        
        
class Optimizer:
    def archive(self) -> None:
        ...

    def merge(self, partitions: List[Partition]) -> List[Partition]:
        ...
    
    