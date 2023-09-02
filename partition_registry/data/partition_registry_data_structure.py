from typing import Tuple
from typing import Dict
from typing import List
import datetime as dt

from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition import Partition

from partition_registry.data.exceptions import UnknownPartitionTypeError


class PartitionRegistryStructure:
    def __init__(
        self,
        events: List[PartitionRegistryEvent]
    ) -> None:
        self.events = self.optimize_event_stream(events)

    def optimize_event_stream(
        self,
        events: List[PartitionRegistryEvent]
    ) -> List[PartitionRegistryEvent]:
        """
        Event stream optimizer allows left only last actions for the spesified interval
        Optimizer complexity is O(n)
        
        Args:
            events (List[PartitionRegistryEvent]): _description_

        Returns:
            List[PartitionRegistryEvent]: _description_
        """
        optimized_event_stream: Dict[Tuple[dt.datetime, dt.datetime], PartitionRegistryEvent] = {}
        
        for event in events:
            key = (event.partition.startpoint, event.partition.endpoint)
            if key not in optimized_event_stream or event.created_date > optimized_event_stream[key].created_date:
                optimized_event_stream[key] = event
        return list(optimized_event_stream.values())

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

        for event in self.events:
            if event.partition.startpoint >= desired_partition.endpoint:
                pass

            if event.partition.endpoint <= desired_partition.startpoint:
                pass

            # If even one unique event is not ready - desired partition
            # is not comprehensevely covered
            if not event.is_ready:
                return False

        return True
