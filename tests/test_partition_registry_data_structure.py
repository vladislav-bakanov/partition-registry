import datetime as dt

import hypothesis.strategies as st
from hypothesis import given

from typing import List

from partition_registry.data.partition import DesiredPartition
from partition_registry.data.partition import ReadyPartition
from partition_registry.data.partition import NotReadyPartition

from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.partition_registry_data_structure import PartitionRegistryDataStructure


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        )
    ])
)
def test_interval_tree_with_one_ready_partition(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is True, \
        f"Based on events <{events}> partition {str(desired_partition)} should be ready, but marked as not ready"


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1)),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_part_of_partition_is_not_ready(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_by_intersected_interval(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            created_date=dt.datetime(2000, 1, 1)
        ),
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_ready_by_intersected_interval(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is True, \
        f"Based on events <{events}> partition {str(desired_partition)} should be ready, but marked as not ready"


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
            created_date=dt.datetime(2001, 1, 1)
        )
    ])
)
def test_interval_tree_when_partitioned_was_not_ready_because_of_partially_cover_intersected_interval(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
            created_date=dt.datetime(2000, 12, 2)
        ),
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
            created_date=dt.datetime(2001, 1, 1)
        ),
        PartitionRegistryEvent(
            NotReadyPartition(dt.datetime(2000, 11, 1), dt.datetime(2000, 11, 2)),
            created_date=dt.datetime(2001, 1, 2)
        ),
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(1999, 11, 1), dt.datetime(2000, 11, 2)),
            created_date=dt.datetime(2001, 1, 4)
        ),
        PartitionRegistryEvent(
            ReadyPartition(dt.datetime(2000, 8, 1), dt.datetime(2001, 1, 1)),
            created_date=dt.datetime(2001, 1, 6)
        )
    ])
)
def test_interval_tree_with_complicated_different_in_time_partition_which_eventually_was_ready(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    prds = PartitionRegistryDataStructure(set(events))
    assert prds.is_partition_ready(desired_partition) is False, \
        f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"
