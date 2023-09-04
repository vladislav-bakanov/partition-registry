import collections

from typing import Union
from typing import Dict
from typing import List

from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition import Partition
from partition_registry.data.provider import Provider

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PostgreSQLSource
from partition_registry.data.source import AirflowDAGSource


class PartitionRegistryStructure:
    def __init__(
        self,
        events: List[PartitionRegistryEvent]
    ) -> None:
        self.formatted_events = self.format_event_stream(events)

    def format_event_stream(
        self,
        events: List[PartitionRegistryEvent]
    ) -> Dict[
        Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource],
        Dict[
            Provider,
            List[PartitionRegistryEvent]
        ]
    ]:
        """
        Event stream optimizer allows left only last actions for the spesified interval
        Optimizer complexity is O(n)

        Args:
            events (List[PartitionRegistryEvent]): _description_

        Returns:
            List[PartitionRegistryEvent]: _description_
        """
        formatted_event_stream: Dict[
            Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource],
            Dict[
                Provider,
                Dict[Partition, PartitionRegistryEvent]
            ]
        ] = {}

        for event in events:
            event.validate()

            if event.source not in formatted_event_stream:
                formatted_event_stream[event.source] = {}

            if event.provider not in formatted_event_stream[event.source]:
                formatted_event_stream[event.source][event.provider] = {}

            if (
                event.partition not in formatted_event_stream[event.source][event.provider]
                or
                event.created_date > formatted_event_stream[event.source][event.provider][event.partition].created_date
            ):
                formatted_event_stream[event.source][event.provider][event.partition] = event

        return {
            source: {provider: list(partition_info.values())}
            for source, provider_info in formatted_event_stream.items()
            for provider, partition_info in provider_info.items()
        }

    def is_partition_ready(
        self,
        source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource],
        providers: List[Provider],
        desired_partition: Partition
    ) -> bool:
        """Is desired partition ready based on known events.

        Args:
            desired_partition (DesiredPartition): partition for that you want to know is that ready or not

        Returns:
            bool: is partition ready
        """

        # In case when we don't know nothing about the partition
        # we can't say that it's ready. Hence we should say that it's not ready
        if source not in self.formatted_events:
            return False

        source_events = self.formatted_events[source]

        # Case when we know there is provider, but we don't have events for it
        if not collections.Counter(providers) == collections.Counter(list(source_events.keys())):
            return False

        for provider in providers:
            provider_events = source_events[provider]
            # In case we don't have provider in the list, but there are no events for the provider
            if not provider_events:
                return False

            total_seconds: float = 0
            for event in provider_events:
                if event.partition.startpoint > desired_partition.endpoint:
                    continue

                if event.partition.endpoint < desired_partition.startpoint:
                    continue

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
