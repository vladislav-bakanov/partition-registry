from typing import Protocol
from typing import Set
from typing import List
from typing import Set
from typing import Dict
from typing import Tuple

from collections import defaultdict
from functools import cached_property

import datetime as dt
import os

from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import UnknownPartition

from partition_registry.data.provider import Provider

from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist

from partition_registry.data.status import SuccededOnAddToQueueStatus
from partition_registry.data.status import FailedOnAddToQueueStatus

from partition_registry.data.status import SuccededPurification
from partition_registry.data.status import FailedPurification


class Queue(Protocol):
    def put(self, partition: LockedPartition | UnlockedPartition) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus: ...
    def pull(self) -> ...: ...
    def purify(self) -> SuccededPurification | FailedPurification: ...
    def persist(self) -> SuccededPersist | FailedPersist: ...

    @property
    def cache_size(self) -> int: ...


class MainQueue(Queue):
    def __init__(
        self,
        persist_size: int,
    ) -> None:
        self.persist_size = persist_size
        restored_queue = defaultdict(set) # self.__restore_persisted_queues()
        self._create_local_cache_file_if_not_exists()
        self.cache: Dict[Provider, Set[LockedPartition | UnlockedPartition]] = restored_queue[0] or defaultdict(set)  # TODO: think about separate class for restored queues

    def __parse_local_cache_row(self, row: str) -> str:
        # TODO: make parser to parse locally persisted records
        ...

    def __restore_persisted_cache(self) -> Tuple[
        Dict[Provider, Set[LockedPartition]],
        Dict[Provider, Set[UnlockedPartition]]
    ]:
        # TODO: make file configurable
        with open('local.cache', 'r', encoding='utf-8') as f:
            ...
        
        return ...

    def put(self, partition: LockedPartition | UnlockedPartition) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus:
        match partition:
            case LockedPartition():
                self.cache[partition.provider].add(partition)
                self.persist_locally(partition)

            case UnlockedPartition():
                if partition.provider not in self.cache:
                    msg = f"Can't find provider ({partition.provider}) within locked partitions queue..."
                    return FailedOnAddToQueueStatus(msg)
            
                try:
                    self.cache[partition.provider].remove(
                        LockedPartition(
                            partition.start,
                            partition.end,
                            partition.provider,
                            partition.created_at
                        )
                    )
                except KeyError as _:
                    msg = f"Can't remove partition {partition} from the locked partitions list due to this partition doesn't exist..."
                    return FailedOnAddToQueueStatus(msg)
                
                self.cache[partition.provider].add(partition)
                
                persist_status = self.persist_locally()
                if isinstance(persist_status, FailedPersist):
                    return FailedOnAddToQueueStatus(persist_status.error_message)

            case _:
                # TODO: make explicit error type
                raise ValueError(f"Unknown partition type. Expected: {LockedPartition.__name__}, {UnlockedPartition.__name__}")
        
        return SuccededOnAddToQueueStatus()

    def purify_cache(self) -> SuccededPurification | FailedPurification:
        try:
            self.cache.clear()
        except Exception as e:
            return FailedPurification(f"Failed purification: {e}")
        
        return SuccededPurification()
    

    def persist(self) -> SuccededPersist | FailedPersist:
        ...

    def _create_local_cache_file_if_not_exists(self) -> None:
        if not os.path.exists('./local.cache'):
            with open('local.cache', 'a+') as f:
                f.write(';'.join(UnlockedPartition.__dataclass_fields__.keys()) + ';')
        else:
            with open('local.cache', 'a+') as f:
                if not f.readlines():
                    f.write(';'.join(UnlockedPartition.__dataclass_fields__.keys()) + ';')


    def persist_locally(self, partition: LockedPartition | UnlockedPartition) -> SuccededPersist | FailedPersist:
        with open('local.cache', 'a+') as f:
            match partition:
                case UnlockedPartition():
                    f.write(f"{partition.provider};{partition.start};{partition.end};{partition.locked_at};{partition.unlocked_at};")
                case LockedPartition():
                    f.write(f"\n{partition.provider};{partition.start};{partition.end};{partition.locked_at};;")
        
        if self.cache_size >= self.persist_size:
            persist_status = self.persist()
            if isinstance(persist_status, FailedPersist):
                return persist_status
            
            purification_status = self.purify_cache()
            if isinstance(purification_status, FailedPurification):
                return FailedPersist(purification_status.error_message)

    @property
    def cache_size(self) -> int:
        return len(self.cache)


from partition_registry.data.source import Source

q = MainQueue(1)
source = Source('prodcloudna-de-production.marketing.trials_hub')
provider = Provider('my_daily_dag', source)
partition1 = LockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2), provider)
partition2 = LockedPartition(dt.datetime(2000, 1, 2), dt.datetime(2000, 1, 4), provider)
q.put(partition1)
q.put(partition2)
print(q.cache_size)
print(q.cache)