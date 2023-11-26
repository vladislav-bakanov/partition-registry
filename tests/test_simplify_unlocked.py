import datetime as dt

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.data.partition import UnlockedPartition


def test_simplify_unlocked_partitions_on_empty_list() -> None:
    registry = PartitionRegistry()
    partitions: set[UnlockedPartition] = []
    
    result = registry.simplify_unlocked(partitions)
    assert result == set(), f"Expected empty set if input is empty set, got: {result}"


def test_simplify_unlocked_partitions_on_intersected_partitions() -> None:
    registry = PartitionRegistry()
    unlocked_partitions = [
        UnlockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10)),
        UnlockedPartition(dt.datetime(2000, 1, 3), dt.datetime(2000, 1, 4)),
    ]
    result = list(registry.simplify_unlocked(unlocked_partitions))

    assert len(result) == 1, f"Expected only one SimplePartition, but got: {result}"
    
    assert result[0].start == dt.datetime(2000, 1, 1) and result[0].end == dt.datetime(2000, 1, 10), \
        f"Expected one SimplePartition(start=datetime(2000, 1, 1), end=datetime(2000, 1, 10)) in result, but got: {result[0]}"
    

def test_simplify_unlocked_partitions_on_mixed_partitions() -> None:
    registry = PartitionRegistry()
    unlocked_partitions = [
        UnlockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10)),
        # Intersected partition
        UnlockedPartition(dt.datetime(2000, 1, 3), dt.datetime(2000, 1, 4)),
        # Not intersected partition
        UnlockedPartition(dt.datetime(2000, 1, 12), dt.datetime(2000, 1, 13)),
    ]
    result = list(registry.simplify_unlocked(unlocked_partitions))
    result.sort(key=lambda x: x.start)

    assert len(result) == 2, f"Expected exact 2 SimplePartitions in result, but got: {result}"
    
    assert result[0].start == dt.datetime(2000, 1, 1) and result[0].end == dt.datetime(2000, 1, 10), \
        f"Expected SimplePartition(start=datetime(2000, 1, 1), end=datetime(2000, 1, 10)) as a first partition in result, but got: {result[0]}"

    assert result[1].start == dt.datetime(2000, 1, 12) and result[1].end == dt.datetime(2000, 1, 13), \
        f"Expected SimplePartition(start=datetime(2000, 1, 12), end=datetime(2000, 1, 13)) as a second partition in result, but got: {result[1]}"
    
def test_simplify_unlocked_partitions_on_not_intersected_partitions() -> None:
    registry = PartitionRegistry()
    unlocked_partitions = [
        # Intersected partition
        UnlockedPartition(dt.datetime(2000, 1, 3), dt.datetime(2000, 1, 4)),
        # Not intersected partition
        UnlockedPartition(dt.datetime(2000, 1, 12), dt.datetime(2000, 1, 13)),
    ]
    result = list(registry.simplify_unlocked(unlocked_partitions))
    result.sort(key=lambda x: x.start)

    assert len(result) == 2, f"Expected exact 2 SimplePartitions in result, but got: {result}"
    
    assert result[0].start == dt.datetime(2000, 1, 3) and result[0].end == dt.datetime(2000, 1, 4), \
        f"Expected SimplePartition(start=datetime(2000, 1, 3), end=datetime(2000, 1, 4)) as a first partition in result, but got: {result[0]}"

    assert result[1].start == dt.datetime(2000, 1, 12) and result[1].end == dt.datetime(2000, 1, 13), \
        f"Expected SimplePartition(start=datetime(2000, 1, 12), end=datetime(2000, 1, 13)) as a second partition in result, but got: {result[1]}"
