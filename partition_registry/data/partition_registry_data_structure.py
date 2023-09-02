from typing import Set
from typing import List

from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition import ReadyPartition
from partition_registry.data.partition import NotReadyPartition
from partition_registry.data.partition import DesiredPartition

from partition_registry.data.exceptions import UnknownPartitionTypeError


class PartitionRegistryDataStructure:
    def __init__(
        self,
        events: Set[PartitionRegistryEvent]
    ) -> None:
        self.events = list(events)
        self.missed_partitions: List[NotReadyPartition] = []
        for event in self.events:
            if isinstance(event.partition, ReadyPartition):
                prev_partition = NotReadyPartition(event.partition.startpoint, event.partition.endpoint)
                if prev_partition in self.missed_partitions:
                    self.missed_partitions.remove(prev_partition)
            elif isinstance(event.partition, NotReadyPartition):
                self.missed_partitions.append(event.partition)
            else:
                raise UnknownPartitionTypeError(f"Expected partition type <NotReadyPartition / ReadyPartition>, got {type(event.partition)}")

    def is_partition_ready(self, desired_partition: DesiredPartition) -> bool:
        """Is desired partition ready based on known events.

        Args:
            desired_partition (DesiredPartition): partition for that you want to know is that ready or not

        Returns:
            bool: is partition ready
        """
        # In case when we don't know nothing about the partition
        # we can't say that it's ready. Hence we should say that it's not ready
        if not self.events:
            return False

        for missed_partition in self.missed_partitions:
            if missed_partition.startpoint <= desired_partition.startpoint <= missed_partition.endpoint:
                return False

        return True
