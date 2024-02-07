import datetime as dt
from dateutil import tz

from partition_registry.actor.registry import EventsRegistry
from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.event import EventType
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.partition import SimplePartition

from partition_registry.data.status import PartitionReady
from partition_registry.data.status import PartitionNotReady
from partition_registry.data.status import FailedRegistration


def check_partition_readiness(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str,
    source_registry: SourceRegistry,
    events_registry: EventsRegistry
) -> PartitionReady | PartitionNotReady | FailedRegistration:
    
    if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
        start = start.astimezone(tz.UTC)
    
    if end.tzinfo is None or end.tzinfo.utcoffset(end) is None:
        end = end.astimezone(tz.UTC)
    
    simple_partition = SimplePartition(start, end)
    simple_source = SimpleSource(source_name)
    match source_registry.lookup_registered(simple_source):
        case None:
            return FailedRegistration(
                f"Can't check readiness. Source with name <<{simple_source.name}>> not registered. "
                "Register source first. "
            )
        case RegisteredSource() as registered_source: ...
    
    partitions = events_registry.get_source_partitions(simple_partition, registered_source)

    # TODO: add test for this case
    if not partitions:
        return PartitionNotReady(
            f"There are no registered partitions by source <<{simple_source.name}>> "
            f"which could meet the conditions of required period: {start} - {end}..."
        )

    # TODO: add test for this case
    events = events_registry.get_last_partition_events(partitions)
    for event in events:
        if event.event_type == EventType.LOCK.value:
            return PartitionNotReady(
                f"Source is locked by: <<partition_id:{event.id}>>..."
            )

    # TODO: add test for this case
    # Case when partition is registered but we don't have any event by this partition
    partitions_presented_in_events = [event.id for event in events]
    real_partitions = [partition for partition in partitions if partition.id in partitions_presented_in_events]
    if not real_partitions:
        return PartitionNotReady(
            f"Source <<{simple_source.name}>> has registered partitions, but "
            f"there are no events which could meet the requirements of desired interval: {start} - {end}..."
        )

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
            f"First partition <<{first_partition.__dict__}>> sorted by <<start>> is greater "
            f"than start point <<{start}>> of desired interval..."
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
            f"Last partition <<{last_partition.__dict__}>> sorted by <<start>> is less "
            f"than end point <<{end}>> of desired interval..."
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
                "Desired interval not comprehesively convered. "
                f"There is a gap between partitions: {current_end}-{sorted_events[current_position].start}"
            )
        current_end = sorted_events[current_position].end
        current_position += 1
        
    return PartitionReady()
