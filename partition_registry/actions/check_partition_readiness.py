import datetime as dt

from partition_registry.actor.events_registry import EventsRegistry
from partition_registry.actor.partition_registry import PartitionRegistry

from partition_registry.data.event import EventType

from partition_registry.data.status import PartitionReady
from partition_registry.data.status import PartitionNotReady


def check_partition_readiness(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str,
    partition_registry: PartitionRegistry,
    events_registry: EventsRegistry,
) -> PartitionReady | PartitionNotReady:
    partitions = partition_registry.get_filtered_partitions(start, end, source_name)

    if not partitions:
        return PartitionNotReady(f"There are no registered partitions by source <<{source_name}>> within the requested interval: <<{start} : {end}>>")

    events = events_registry.get_partition_events(partitions)
    if not events:
        return PartitionNotReady(f"There are no registered events by source <<{source_name}>> within the requested interval: <<{start} : {end}>> ")

    for event in events:
        if event.event_type == EventType.LOCK.value:
            return PartitionNotReady(f"Source is locked by: <<partition_id:{event.id}>>...")

    
    # Case when partition is registered but we don't have any event by this partition
    partitions_presented_in_events = [event.id for event in events]
    real_partitions = [partition for partition in partitions if partition.id in partitions_presented_in_events]
    if not real_partitions:
        return PartitionNotReady(f"There are no events by registered partitions for Source <<{source_name}>> within the requested interval: <<{start} : {end}>>")

    # Case when first partition partition start date is greater than interval
    # and due to that fact can't be comprehensively covered
    # It's look like:
    #
    # v
    # | desired_partition |
    #    |    p_1   ||   p_2   | ... |    p_n    |
    #    ^
    # TODO: add test for this case
    first_partition = min(real_partitions, key=lambda p: p.start)
    if first_partition.start > start:
        return PartitionNotReady(
            "Requested interval not comprehensively covered by registered partitions..."
            f"\nThere are no partitions to cover interval: <<{start} : {first_partition.start}>>"
        )

    # Case when intersected partition start date is less than end date in desired partition
    # and due to that fact can't be comprehensively covered
    # It's look like:
    #                                                v
    #                            | desired_partition |
    # |    p_1   ||   p_2   | ... |    p_n    |
    #                                         ^
    # TODO: add test for this case
    last_partition = max(real_partitions, key=lambda p: p.end)
    if last_partition.end < end:
        return PartitionNotReady(
            "Requested interval not comprehensively covered by registered partitions..."
            f"\nThere are no partitions to cover interval: <<{last_partition.end} : {end}>>"
        )

    sorted_events = sorted(real_partitions, key=lambda p: p.start)
    current_end = sorted_events[0].end
    current_position = 1
    total_events = len(sorted_events)

    # We return False here only in case when we found a gap between 2 consistent partitions
    # Examples:
    # 1. Desired interval: |2000-01-01: 2000-01-04|
    #    Partitions: |2000-01-01: 2000-01-02|, |2000-01-01: 2000-01-04| - should comprehensively cover interval
    #
    # 2. Desired interval: |2000-01-01: 2000-01-04|
    #    Partitions: |2000-01-01: 2000-01-02|, |2000-01-03: 2000-01-04| - shouldn't comprehensively cover interval
    #
    # In terms of Big O we have O(n) complexity here for the worst case
    # TODO: add test for this case
    while current_position < total_events:
        if current_end < sorted_events[current_position].start:
            return PartitionNotReady(
                "Requested interval not comprehesively convered..."
                f"There are not events to cover interval: <<{current_end} : {sorted_events[current_position].start}>>"
            )
        current_end = sorted_events[current_position].end
        current_position += 1

    return PartitionReady()
