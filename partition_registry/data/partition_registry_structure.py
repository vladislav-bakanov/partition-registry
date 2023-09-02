from typing import Tuple
from typing import Dict
from typing import List
import datetime as dt

from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition import Partition
from partition_registry.data.source import Source
from partition_registry.data.partition import PartitionSize
from partition_registry.data.event_state import EventState


class PartitionRegistryStructure:
    def __init__(
        self,
        # source: Source,
        events: List[PartitionRegistryEvent]
    ) -> None:
        # self.source = source
        self.events = self.optimize_event_stream(events)

    def optimize_event_stream(
        self,
        events: List[PartitionRegistryEvent]
    ) -> Dict[PartitionSize, List[PartitionRegistryEvent]]:
        """
        Event stream optimizer allows left only last actions for the spesified interval
        Optimizer complexity is O(n)
        
        Args:
            events (List[PartitionRegistryEvent]): _description_

        Returns:
            List[PartitionRegistryEvent]: _description_
        """
        optimized_event_stream: Dict[PartitionSize, Dict[Partition, PartitionRegistryEvent]] = {}

        for event in events:
            event.validate()
            # if event.partition.source != self.source:
            #     continue

            partition_size = PartitionSize(event.partition.size_in_sec)
            if partition_size not in optimized_event_stream:
                optimized_event_stream[partition_size] = {}
            
            if (
                event.partition not in optimized_event_stream[partition_size]
                or
                event.created_date > optimized_event_stream[partition_size][event.partition].created_date
            ):
                optimized_event_stream[partition_size][event.partition] = event

        return {partition_size: list(v.values()) for partition_size, v in optimized_event_stream.items()}

    def is_partition_ready(self, desired_partition: Partition) -> bool:
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

        for _, events in self.events.items():
            total_seconds: float = 0
            for event in events:
                if event.partition.startpoint > desired_partition.endpoint:
                    pass

                if event.partition.endpoint < desired_partition.startpoint:
                    pass

                if not event.is_ready:
                    return False

                if event.partition.startpoint < desired_partition.startpoint:
                    event.partition.startpoint = desired_partition.startpoint

                if event.partition.endpoint > desired_partition.endpoint:
                    event.partition.endpoint = desired_partition.endpoint

                total_seconds += event.partition.size_in_sec

            if total_seconds != desired_partition.size_in_sec:
                return False

        return True
