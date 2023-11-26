import datetime as dt
from collections import defaultdict

from partition_registry.orm import PartitionRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import is_intersected

from partition_registry.data.status import FailedUnlock


class PartitionRegistry:
    def __init__(
        self,
        # session: Session,
    ) -> None:
        # self.session = session
        self.table = PartitionRegistryORM
        self.locked: dict[RegisteredSource, dict[RegisteredProvider, set[LockedPartition]]] = defaultdict()
        self.unlocked: dict[RegisteredSource, dict[RegisteredProvider, set[UnlockedPartition]]] = defaultdict()

    def lock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition
    ) -> LockedPartition:
        locked_partition = LockedPartition(partition.start, partition.end, partition.created_at)
        self.locked[source] = defaultdict(set)
        self.locked[source][provider].add(locked_partition)
        return locked_partition
    
    def unlock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> UnlockedPartition | FailedUnlock:
        locked_partition = self.find_locked_partition(source, provider, partition.start, partition.end)
        if not locked_partition:
            return FailedUnlock(f"{partition} was not found among locked...")
        
        unlocked_partition = UnlockedPartition(locked_partition.start, locked_partition.end, locked_partition.created_at, locked_partition.locked_at)
        self.locked[source][provider].remove(locked_partition)
        
        if source in self.unlocked:
            if provider in self.unlocked[source]:
                self.unlocked[source][provider].add(unlocked_partition)
            else:
                self.unlocked[source] = {provider: set([unlocked_partition])}
        else:
            self.unlocked = {source: {provider: set([unlocked_partition])}}

        return unlocked_partition

    def is_partition_locked(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition,
    ) -> bool:
        return partition in self.locked.get(source, {}).get(provider, set())
    
    def find_locked_partition(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        start: dt.datetime,
        end: dt.datetime,
    ) -> LockedPartition | None:
        locked_partitions = self.locked.get(source, {}).get(provider, set())
        for locked_partition in locked_partitions:
            if locked_partition.start == start and locked_partition.end == end:
                return locked_partition
        return None

    def get_locked_partitions_by_source(self, source: RegisteredSource) -> set[LockedPartition]:
        return {
            locked_partition
            for _, locked_partitions in self.locked.get(source, {}).items()
            for locked_partition in locked_partitions
        }
    
    def get_unlocked_partitions_by_source(self, source: RegisteredSource) -> set[UnlockedPartition]:
        return {
            unlocked_partition
            for _, unlocked_partitions in self.unlocked.get(source, {}).items()
            for unlocked_partition in unlocked_partitions
        }

    def simplify_unlocked(
        self,
        partitions: list[UnlockedPartition]
    ) -> set[SimplePartition]:
        """Union all intersected partitions in one set to iterate quicker over it"""
        result: list[SimplePartition] = []
        partitions.sort(key=lambda x: x.start)
        if len(partitions) == 0:
            return set()

        current = SimplePartition(partitions[0].start, partitions[0].end)
        for partition in partitions[1:]:
            if is_intersected(partition, current):
                start, end = current.start, max(partition.end, current.end)
            else:
                start, end = partition.start, partition.end
                result.append(current)

            current = SimplePartition(start, end)
        
        if len(result) == 0:
            return {current, }
        
        if result[-1] != current:
            result.append(current)

        return set(result)



    def is_partition_ready(
        self,
        source: RegisteredSource,
        partition: SimplePartition
    ) -> bool:
        source_locked_partitions = self.get_locked_partitions_by_source(source)
        source_unlocked_partitions = self.get_unlocked_partitions_by_source(source)
        for lp in source_locked_partitions:
            if is_intersected(partition, lp):
                return False

        simplified_unlocked_partitions = [
            unlocked_partition
            for unlocked_partition in self.simplify_unlocked(list(source_unlocked_partitions))
            if is_intersected(partition, unlocked_partition)
        ]
        
        i = 1
        current_end = simplified_unlocked_partitions[0].end
        while i < len(simplified_unlocked_partitions):
            if current_end < simplified_unlocked_partitions[i].start:
                return False
            current_end = simplified_unlocked_partitions[i].end
        
        if current_end < partition.end:
            return False
        
        return True
                