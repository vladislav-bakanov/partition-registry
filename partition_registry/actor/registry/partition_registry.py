import datetime as dt
import pytz
from collections import defaultdict
from typing import Optional
from typing import SupportsIndex

import uuid

from sqlalchemy.orm import Session
from partition_registry.orm import PartitionRegistryORM


from partition_registry.data.registry import Registry
from partition_registry.data.partition import Partition
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import SimplePartition

from partition_registry.data.status import Status
from partition_registry.data.status import FailedLock
from partition_registry.data.status import SuccededLock
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedUnlock


class PartitionRegistry:
    def __init__(
        self,
        # session: Session,
        table: type[PartitionRegistryORM]
    ) -> None:
        # self.session = session
        self.table = table
        self.cache: dict[
            tuple[RegisteredSource, RegisteredProvider],
            set[LockedPartition | UnlockedPartition]
        ] = defaultdict(set)

    def lock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition
    ) -> LockedPartition:
        locked_partition = LockedPartition(partition.start, partition.end, partition.created_at)
        self.cache[(source, provider)].add(locked_partition)
        return locked_partition
    
    def unlock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> UnlockedPartition | FailedUnlock:
        locked_partition = self.find_locked_partition(source, provider, partition)
        if not locked_partition:
            return FailedUnlock(f"{partition} was not found among locked...")
        
        unlocked_partition = UnlockedPartition(locked_partition.start, locked_partition.end, locked_partition.created_at, locked_partition.locked_at)
        self.cache[(source, provider)].remove(locked_partition)
        self.cache[(source, provider)].add(unlocked_partition)
        return unlocked_partition


    def is_partition_locked(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition,
    ) -> bool:
        return partition in self.cache.get((source, provider), set())
    
    def find_locked_partition(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> Optional[LockedPartition]:
        for p in self.cache.get((source, provider), set()):
            if isinstance(p, LockedPartition) and p.start == partition.start and p.end == partition.end:
                return p
        return None
