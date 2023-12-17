import logging
from collections import defaultdict

from redis import Redis

from partition_registry.orm import PartitionRegistryORM

from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import is_intersected
from partition_registry.data.func import safe_parse_datetime
from partition_registry.data.func import generate_unixtime

from partition_registry.data.status import FailedUnlock
from partition_registry.data.status import SuccededLock


class PartitionRegistry:
    def __init__(
        self, redis: Redis
    ) -> None:
        self.redis = redis
        self.redis_path = "registry:partition"
        
        self.locked_cache: dict[RegisteredSource, dict[RegisteredProvider, set[LockedPartition]]] = defaultdict()
        self.unlocked_cache: dict[RegisteredSource, dict[RegisteredProvider, set[UnlockedPartition]]] = defaultdict()

    def memory_lookup_locked(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> LockedPartition | None:
        source_cache = self.locked_cache.get(source, {}) or {}
        provider_cache = source_cache.get(provider, set()) or set()

        for locked_partition in provider_cache:
            if locked_partition.start == partition.start and locked_partition.end == partition.end:
                return locked_partition

    def redis_lookup_locked(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> LockedPartition | None:
        redis_lock_key = (
            f"{self.redis_path}:{source.name}:{provider.name}:locked:"
            f"{generate_unixtime(partition.start)}-{generate_unixtime(partition.end)}"
        )
        match self.redis.hscan(redis_lock_key):
            case int() as cursor, {b'created_at': bytes(created_at_bstring), b'locked_at': bytes(locked_at_bstring)}:
                created_at = safe_parse_datetime(created_at_bstring)
                locked_at = safe_parse_datetime(locked_at_bstring)
                return LockedPartition(partition.start, partition.end, created_at, locked_at)

    def safe_add_into_memory_cache(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition | UnlockedPartition,
    ) -> None:
        match partition:
            case LockedPartition() as locked_partition:
                if source not in self.locked_cache or provider not in self.locked_cache[source]:
                    self.locked_cache[source] = {provider: {locked_partition, }}
                else:
                    self.locked_cache[source][provider].add(locked_partition)

            case UnlockedPartition() as unlocked_partition:
                if source not in self.unlocked_cache or provider not in self.unlocked_cache[source]:
                    self.unlocked_cache[source] = {provider: {unlocked_partition, }}
                else:
                    self.unlocked_cache[source][provider].add(unlocked_partition)            

    def safe_add_into_redis(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition | UnlockedPartition,
    ) -> None:
        match partition:
            case LockedPartition() as locked_partition:
                redis_lock_key = (
                    f"{self.redis_path}:{source.name}:{provider.name}:locked:"
                    f"{generate_unixtime(locked_partition.start)}-{generate_unixtime(locked_partition.end)}"
                )
                locked_values = {
                    'created_at': str(locked_partition.created_at),
                    'locked_at': str(locked_partition.locked_at),
                }
                if self.redis.hset(redis_lock_key, mapping=locked_values) != len(locked_values):
                    raise ValueError(f"Some fields in Redis have not been updated for the key: {redis_lock_key}")
            
            case UnlockedPartition() as unlocked_partition:
                redis_unlock_key = (
                    f"{self.redis_path}:{source.name}:{provider.name}:unlocked:"
                    f"{generate_unixtime(unlocked_partition.start)}-{generate_unixtime(unlocked_partition.end)}"
                )
                unlocked_values = {
                    'created_at': str(unlocked_partition.created_at),
                    'locked_at': str(unlocked_partition.locked_at),
                    'unlocked_at': str(unlocked_partition.unlocked_at)
                }
                if self.redis.hset(redis_unlock_key, mapping=unlocked_values) != len(unlocked_values):
                    raise ValueError(f"Some fields in Redis have not been updated for the key: {redis_unlock_key}")

    def safe_remove_from_redis(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition | UnlockedPartition
    ) -> None:
        breakpoint()
        match partition:
            case LockedPartition() as locked_partition:
                key = (
                    f"{self.redis_path}:{source.name}:{provider.name}:locked:"
                    f"{generate_unixtime(locked_partition.start)}-{generate_unixtime(locked_partition.end)}"
                )
            case UnlockedPartition() as unlocked_partition:
                key = (
                    f"{self.redis_path}:{source.name}:{provider.name}:unlocked:"
                    f"{generate_unixtime(unlocked_partition.start)}-{generate_unixtime(unlocked_partition.end)}"
                )
        if self.redis.delete(key) != 1:
            raise ValueError(f"Coudln't delete data from Redis by key: {key}")

    def safe_remove_from_memory(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: LockedPartition | UnlockedPartition
    ) -> None:
        match partition:
            case LockedPartition() as locked_partition:
                if source in self.locked_cache and provider in self.locked_cache[source]:
                    self.locked_cache[source][provider].remove(locked_partition)
            case UnlockedPartition() as unlocked_partition:
                if source in self.unlocked_cache and provider in self.unlocked_cache[source]:
                    self.unlocked_cache[source][provider].remove(unlocked_partition)

    def lock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition
    ) -> LockedPartition:
        match self.memory_lookup_locked:
            case LockedPartition() as locked_partition:
                return locked_partition

        match self.redis_lookup_locked(source, provider, partition):
            case LockedPartition() as locked_partition:
                self.safe_add_into_memory_cache(source, provider, locked_partition)
                return locked_partition

        locked_partition = LockedPartition.parse(partition)
        self.safe_add_into_redis(source, provider, locked_partition)
        self.safe_add_into_memory_cache(source, provider, locked_partition)
        return locked_partition

    def unlock(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> UnlockedPartition | FailedUnlock:
        locked_partition = (
            self.memory_lookup_locked(source, provider, partition)
            or
            self.redis_lookup_locked(source, provider, partition)
        )

        if locked_partition is None:
            return FailedUnlock(f"{partition} was not found among locked...")

        unlocked_partition = UnlockedPartition.parse(locked_partition)
        self.safe_add_into_redis(source, provider, unlocked_partition)
        self.safe_add_into_memory_cache(source, provider, unlocked_partition)
        self.safe_remove_from_memory(source, provider, locked_partition)
        self.safe_remove_from_redis(source, provider, locked_partition)
        return unlocked_partition

    def is_partition_locked(
        self,
        source: RegisteredSource,
        provider: RegisteredProvider,
        partition: SimplePartition,
    ) -> bool:
        return (
            self.memory_lookup_locked(source, provider, partition) is not None
            or
            self.redis_lookup_locked(source, provider, partition) is not None
        )

    # TODO: REMASTER!!!
    def get_locked_partitions_by_source(self, source: RegisteredSource) -> set[LockedPartition]:
        return {
            locked_partition
            for _, locked_partitions in self.locked.get(source, {}).items()
            for locked_partition in locked_partitions
        }
    
    # TODO: REMASTER!!!
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
                