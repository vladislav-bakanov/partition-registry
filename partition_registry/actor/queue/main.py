import os
from collections import defaultdict
from contextlib import closing

from sqlalchemy.orm.session import Session

from partition_registry.actor.registry.partition.orm import PartitionRegistryORM
from partition_registry.data.queue import Queue
from partition_registry.data.provider import Provider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition

# Statuses
from partition_registry.data.status import SuccededOnAddToQueueStatus
from partition_registry.data.status import FailedOnAddToQueueStatus
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist
from partition_registry.data.status import SuccededPurification
from partition_registry.data.status import FailedPurification



class MainQueue(Queue):
    def __init__(
        self,
        persist_size: int,
        table: type[PartitionRegistryORM],
        session: Session, # TODO: check article about threadsafe: "See session_faq_threadsafe for background."
    ) -> None:
        self.session = session
        self.persist_size = persist_size
        restored_queue: dict[Provider, set[LockedPartition | UnlockedPartition]] = defaultdict(set) # TODO: fill in this queue by some local ministorage
        self._create_local_cache_file_if_not_exists()
        self.records: dict[Provider, set[LockedPartition | UnlockedPartition]] = restored_queue or defaultdict(set)  # TODO: think about separate class for restored queues

    def put(self, partition: LockedPartition | UnlockedPartition) -> SuccededOnAddToQueueStatus | FailedOnAddToQueueStatus:
        match partition:
            case LockedPartition():
                self.records[partition.provider].add(partition)
                self.persist_locally(partition)

            case UnlockedPartition():
                if partition.provider not in self.records:
                    msg = f"Can't find provider ({partition.provider}) within locked partitions queue..."
                    return FailedOnAddToQueueStatus(msg)
            
                try:
                    self.records[partition.provider].remove(
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
                
                self.records[partition.provider].add(partition)
                
                persist_status = self.persist_locally()
                if isinstance(persist_status, FailedPersist):
                    return FailedOnAddToQueueStatus(persist_status.error_message)

            case _:
                # TODO: make explicit error type
                raise ValueError(f"Unknown partition type. Expected: {LockedPartition.__name__}, {UnlockedPartition.__name__}")
        
        return SuccededOnAddToQueueStatus()

    def purify(self) -> SuccededPurification | FailedPurification:
        try:
            self.records.clear()
        except Exception as e:
            return FailedPurification(f"Failed purification: {e}")
        
        return SuccededPurification()
    

    def persist(self) -> SuccededPersist | FailedPersist:
        db_objects: list[PartitionRegistryORM] = [
            PartitionRegistryORM(
                source=provider.source.name,
                provider=provider.name,
                locked=isinstance(partition, LockedPartition),
                created_at=partition.created_at,
                locked_at=partition.locked_at,
                unlocked_at=None if not isinstance(partition, UnlockedPartition) else partition.unlocked_at
            )
            for provider, partitions in self.records.items()
            for partition in partitions
        ]

        return FailedPersist("DONT FORGET ME")

    def _create_local_cache_file_if_not_exists(self) -> None:
        if not os.path.exists('./local.cache'):
            with open('local.cache', 'a+') as f:
                f.write(';'.join(UnlockedPartition.__dataclass_fields__.keys()) + ';')
        else:
            with open('local.cache', 'a+') as f:
                if not f.readlines():
                    f.write(';'.join(UnlockedPartition.__dataclass_fields__.keys()) + ';')


    def persist_locally(self, partition: LockedPartition | UnlockedPartition) -> SuccededPersist | FailedPersist:
        with closing(open('local.cache', 'a+')) as f:
            match partition:
                case UnlockedPartition():
                    f.write(f"{partition.provider};{partition.start};{partition.end};{partition.locked_at};{partition.unlocked_at};")
                case LockedPartition():
                    f.write(f"\n{partition.provider};{partition.start};{partition.end};{partition.locked_at};;")
        
        if self.size >= self.persist_size:
            persist_status = self.persist()
            if isinstance(persist_status, FailedPersist):
                return persist_status
            
            purification_status = self.purify()
            if isinstance(purification_status, FailedPurification):
                return FailedPersist(purification_status.error_message)

        return SuccededPersist()

    @property
    def size(self) -> int:
        return len(self.records)
