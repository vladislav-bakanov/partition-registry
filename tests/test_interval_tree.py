import datetime as dt

import hypothesis.strategies as st
from hypothesis import given

from typing import List

from partition_registry.data.partition import DesiredPartition
from partition_registry.data.partition import SourcePartition
from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.interval_tree import IntervalTree


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), True),
            created_date=dt.datetime(2000, 12, 2)
        )
    ])
)
def test_interval_tree_with_one_ready_partition(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in events:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready) == desired_partition.size_in_sec


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    nodes=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), True),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1), False),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_part_of_partition_is_not_ready(
    desired_partition: DesiredPartition,
    nodes: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in nodes:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready) != desired_partition.size_in_sec


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    nodes=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), True),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31), False),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_by_intersected_interval(
    desired_partition: DesiredPartition,
    nodes: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in nodes:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready) != desired_partition.size_in_sec


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    nodes=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), False),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31), True),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_ready_by_intersected_interval(
    desired_partition: DesiredPartition,
    nodes: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in nodes:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready) == desired_partition.size_in_sec


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    nodes=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), False),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1), True),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_because_of_partially_cover_intersected_interval(
    desired_partition: DesiredPartition,
    nodes: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in nodes:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready is True) != desired_partition.size_in_sec


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    nodes=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1), False),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1), True),
            created_date=dt.datetime(2001, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 11, 1), dt.datetime(2000, 11, 2), False),
            created_date=dt.datetime(2001, 1, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1999, 11, 1), dt.datetime(2000, 11, 2), True),
            created_date=dt.datetime(2001, 1, 4)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 8, 1), dt.datetime(2001, 1, 1), True),
            created_date=dt.datetime(2001, 1, 6)
        )
    ])
)
def test_interval_tree_with_complicated_different_in_time_partition_which_eventually_was_ready(
    desired_partition: DesiredPartition,
    nodes: List[PartitionRegistryEvent]
) -> None:
    it = IntervalTree(desired_partition)
    for node in nodes:
        it.add_node(node)
    assert sum(node.partition.size_in_sec for node in it.nodes if node.partition.is_ready is True) != desired_partition.size_in_sec
