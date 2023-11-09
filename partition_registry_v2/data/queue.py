from typing import Protocol
from typing import Set
from typing import List
from typing import Set
from typing import Dict
from typing import Tuple

from collections import defaultdict

import datetime as dt

from partition_registry_v2.data.partition import LockedPartition
from partition_registry_v2.data.partition import UnlockedPartition
from partition_registry_v2.data.partition import UnknownPartition

from partition_registry_v2.data.provider import Provider

from partition_registry_v2.data.status import SuccededPersist
from partition_registry_v2.data.status import FailedPersist

from partition_registry_v2.data.status import SuccededOnAddToQueueStatus
from partition_registry_v2.data.status import FailedOnAddToQueueStatus

from partition_registry_v2.data.status import SuccededPurification
from partition_registry_v2.data.status import FailedPurification


class Queue(Protocol):
    def put(self, partition: LockedPartition | UnlockedPartition) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus: ...
    def pull(self) -> ...: ...
    def purify(self) -> SuccededPurification | FailedPurification: ...
    def persist(self) -> SuccededPersist | FailedPersist: ...

    def __len__(self) -> int: ...


class MainQueue(Queue):
    def __init__(
        self,
        persist_size: int,
    ) -> None:
        self.persist_size = persist_size
        restored_queue = self.__restore_persisted_queues()
        self.locked_queue: Dict[Provider, Set[LockedPartition]] = restored_queue[0] or defaultdict(set)  # TODO: think about separate class for restored queues
        self.unlocked_queue: Dict[Provider, Set[UnlockedPartition]] = restored_queue[1] or defaultdict(set)

    def __parse_local_cache_row(self, row: str) -> str:
            # TODO: make parser to parse locally persisted records
            ...

    def __restore_persisted_queues(self) -> Tuple[
        Dict[Provider, Set[LockedPartition]],
        Dict[Provider, Set[UnlockedPartition]]
    ]:
        queue: Dict[Provider, Set[UnlockedPartition]] = defaultdict(set)
        with open('local.cache', 'r', encoding='utf-8') as f:  # TODO: make file configurable
            ...
        
        return ...

    def put(
        self,
        provider: Provider,
        partition: LockedPartition | UnlockedPartition
    ) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus:
        match partition:
            case LockedPartition():
                self.locked_queue[provider].add(partition)
                self.persist_locally()

            case UnlockedPartition():
                try:
                    provider = self.locked_queue[provider]
                except KeyError as _:
                    msg = f"Can't find provider ({provider}) within locked partitions queue..."
                    return FailedOnAddToQueueStatus(msg)
                
                locked_partition = LockedPartition(partition.start, partition.end, partition.created_at)
                try:
                    self.locked_queue[provider].remove(locked_partition)
                except KeyError as _:
                    msg = f"Can't remove partition {partition} from the locked partitions list due to this partition doesn't exist..."
                    return FailedOnAddToQueueStatus(msg)
                
                self.unlocked_queue[provider].add(partition)
                
                persist_status = self.persist_locally()
                if isinstance(persist_status, FailedPersist):
                    return FailedOnAddToQueueStatus(persist_status.error_message)

            case _:
                raise ValueError(f"Unknown partition type. Expected: {LockedPartition.__name__}, {UnlockedPartition.__name__}") # TODO: make explicit error type
        
        return SuccededOnAddToQueueStatus()

    def persist(self) -> SuccededPersist | FailedPersist:
        ...

    def persist_locally(self) -> SuccededPersist | FailedPersist:
        return self.persist()

    def __len__(self) -> int:
        return len(self.locked_queue) + len(self.unlocked_queue)