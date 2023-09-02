import datetime as dt

import hypothesis.strategies as st
from hypothesis import given

from typing import List

from partition_registry.data.partition import Partition
from partition_registry.data.event_state import EventState
from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition_registry_structure import PartitionRegistryStructure


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.READY,
            dt.datetime(2000, 12, 2)
        )
    ])
)
def test_interval_tree_with_one_ready_partition(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is True, \
        f"Based on events <{events}> partition {str(desired_partition)} should be ready, but marked as not ready"


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.READY,
            dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1)),
            EventState.NOT_READY,
            dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_part_of_partition_is_not_ready(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.READY,
            dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            EventState.NOT_READY,
            dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_by_intersected_interval(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            EventState.NOT_READY,
            dt.datetime(2001, 1, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.NOT_READY,
            dt.datetime(2001, 1, 5)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            EventState.READY,
            dt.datetime(2001, 1, 10)
        )
    ])
)
def test_interval_tree_when_partitioned_was_ready_by_intersected_interval(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.NOT_READY,
            dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
            EventState.READY,
            dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_because_of_partially_cover_intersected_interval(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            EventState.NOT_READY,
            dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
            EventState.READY,
            dt.datetime(2001, 1, 1)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 11, 1), dt.datetime(2000, 11, 2)),
            EventState.NOT_READY,
            dt.datetime(2001, 1, 2)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(1999, 11, 1), dt.datetime(2000, 11, 2)),
            EventState.READY,
            dt.datetime(2001, 1, 4)
        ),
        PartitionRegistryEvent(
            Partition(dt.datetime(2000, 8, 1), dt.datetime(2001, 1, 1)),
            EventState.READY,
            dt.datetime(2001, 1, 6)
        )
    ])
)
def test_interval_tree_with_complicated_different_in_time_partition_which_eventually_was_ready(
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"
