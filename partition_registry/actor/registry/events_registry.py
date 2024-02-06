from sqlalchemy.orm import Session

from partition_registry.orm import PartitionEventsORM

from partition_registry.data.registry import Registry

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import is_intersected
from partition_registry.data.func import safe_parse_datetime
from partition_registry.data.func import generate_unixtime

from partition_registry.data.status import FailedUnlock
from partition_registry.data.status import SuccededLock
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist

from partition_registry.data.event import PartitionEvent
from partition_registry.data.event import RegisteredPartitionEvent


class EventsRegistry(Registry[PartitionEvent, RegisteredPartitionEvent]):
    def __init__(self, session: Session) -> None:
        self.session = session
        self.events_table = PartitionEventsORM
        self.cache: dict[PartitionEvent, RegisteredPartitionEvent] = dict()

    def safe_register(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartitionEvent:

        match self.lookup_registered(partition, source, provider):
            case RegisteredPartitionEvent() as registered_event:
                return registered_event
        
        
        
        registered_event = RegisteredPartitionEvent(
            
        )
        
    def lookup_registered(
        self,
        partition: SimplePartition,
        source: RegisteredSource,
        provider: RegisteredProvider
    ) -> RegisteredPartitionEvent | None:
        ...
        
        
        
        
        
        
        
#         match db_

#     def lookup_registered(self, partition: SimplePartition) -> RegisteredPartition | None:
#         return self.memory_lookup(partition) or self.db_lookup(partition)
    
#     def memory_lookup(self, partition: SimplePartition) -> RegisteredPartition | None:
#         return self.cache.get(partition)
    
#     def db_lookup(self, partition: SimplePartition) -> RegisteredPartition | None:
#         session = self.session
#         rows = (
#             session
#             .query(self.table)
#             .filter(self.table.start =)
#         )

#     def persist_to_db(self, partition: RegisteredPartition) -> SuccededPersist | FailedPersist:
#         ...

    # def lock(
    #     self,
    #     source: RegisteredSource,
    #     provider: RegisteredProvider,
    #     partition: SimplePartition
    # ) -> LockedPartition:
    #     match self.memory_lookup_locked:
    #         case LockedPartition() as locked_partition:
    #             return locked_partition

    #     match self.redis_lookup_locked(source, provider, partition):
    #         case LockedPartition() as locked_partition:
    #             self.push_to_memory(source, provider, locked_partition)
    #             return locked_partition

    #     locked_partition = LockedPartition.parse(partition)
    #     self.safe_add_into_redis(source, provider, locked_partition)
    #     self.safe_add_into_memory_cache(source, provider, locked_partition)
    #     return locked_partition

    # def unlock(
    #     self,
    #     source: RegisteredSource,
    #     provider: RegisteredProvider,
    #     partition: SimplePartition,
    # ) -> UnlockedPartition | FailedUnlock:
    #     locked_partition = (
    #         self.memory_lookup_locked(source, provider, partition)
    #         or
    #         self.redis_lookup_locked(source, provider, partition)
    #     )

    #     if locked_partition is None:
    #         return FailedUnlock(f"{partition} was not found among locked...")

    #     unlocked_partition = UnlockedPartition.parse(locked_partition)
    #     self.safe_add_into_redis(source, provider, unlocked_partition)
    #     self.safe_add_into_memory_cache(source, provider, unlocked_partition)
    #     self.safe_remove_from_memory(source, provider, locked_partition)
    #     self.safe_remove_from_redis(source, provider, locked_partition)
    #     return unlocked_partition

    # def is_partition_locked(
    #     self,
    #     source: RegisteredSource,
    #     provider: RegisteredProvider,
    #     partition: SimplePartition,
    # ) -> bool:
    #     return (
    #         self.memory_lookup_locked(source, provider, partition) is not None
    #         or
    #         self.redis_lookup_locked(source, provider, partition) is not None
    #     )

    # # TODO: REMASTER!!!
    # def get_locked_partitions_by_source(self, source: RegisteredSource) -> set[LockedPartition]:
    #     return {
    #         locked_partition
    #         for _, locked_partitions in self.locked.get(source, {}).items()
    #         for locked_partition in locked_partitions
    #     }
    
    # # TODO: REMASTER!!!
    # def get_unlocked_partitions_by_source(self, source: RegisteredSource) -> set[UnlockedPartition]:
    #     return {
    #         unlocked_partition
    #         for _, unlocked_partitions in self.unlocked.get(source, {}).items()
    #         for unlocked_partition in unlocked_partitions
    #     }

    # def simplify_unlocked(
    #     self,
    #     partitions: list[UnlockedPartition]
    # ) -> set[SimplePartition]:
    #     """Union all intersected partitions in one set to iterate quicker over it"""
    #     result: list[SimplePartition] = []
    #     partitions.sort(key=lambda x: x.start)
    #     if len(partitions) == 0:
    #         return set()

    #     current = SimplePartition(partitions[0].start, partitions[0].end)
    #     for partition in partitions[1:]:
    #         if is_intersected(partition, current):
    #             start, end = current.start, max(partition.end, current.end)
    #         else:
    #             start, end = partition.start, partition.end
    #             result.append(current)

    #         current = SimplePartition(start, end)
        
    #     if len(result) == 0:
    #         return {current, }
        
    #     if result[-1] != current:
    #         result.append(current)

    #     return set(result)
                