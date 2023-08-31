from typing import Union
from typing import List

import datetime as dt

from unittest.mock import MagicMock

import hypothesis.strategies as st
from hypothesis import given
from hypothesis import assume

from partition_registry.controller.partition_registry import PartitionRegistryController
from partition_registry.data.partition import DesiredPartition
from partition_registry.data.partition import SourcePartition

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PentahoSource
from partition_registry.data.source import AirflowDAGSource

from partition_registry.data.partition_registry_event import PartitionRegistryEvent

from tests.arbitrary.source import arb_bigquery_source
from tests.arbitrary.source import arb_pentaho_source
from tests.arbitrary.source import arb_airflow_dag_source

from tests.arbitrary.source import arb_partitioned_bigquery_source
from tests.arbitrary.source import arb_partitioned_pentaho_source
from tests.arbitrary.source import arb_partitioned_ariflow_dag_source
from tests.arbitrary.source import arb_not_partitioned_bigquery_source
from tests.arbitrary.source import arb_not_partitioned_pentaho_source
from tests.arbitrary.source import arb_not_partitioned_ariflow_dag_source


@given(
    desired_partition=st.builds(DesiredPartition),
    events=st.just([])
)
def test_readiness_if_there_are_no_events(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent],
) -> None:
    """Test decision-maker in case when there are no events in Partition Registry for source"""
    assume(desired_partition.startpoint < desired_partition.endpoint)
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is False, \
        f"Decision-maker decided that {desired_partition} is ready, but list of events is empty, partition can't be ready..."


@given(
    source=st.one_of(arb_partitioned_bigquery_source, arb_partitioned_pentaho_source, arb_partitioned_ariflow_dag_source),
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 1, 16), False),
            created_date=dt.datetime(2000, 1, 1, 16, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1, 0), dt.datetime(2000, 1, 1, 16), True),
            created_date=dt.datetime(2000, 1, 1, 17, 0)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2), False),
            created_date=dt.datetime(2000, 1, 2, 0, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2), True),
            created_date=dt.datetime(2000, 1, 2, 3, 0)
        ),
    ])
)
def test_partitioned_source_if_all_events_are_ready(
    source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test decision-maker in case when Partition Registry events comprehensively covere desired partition and events can intersect.
    """
    print(f"Desired partition: {desired_partition}")
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is True, \
        f"Desired partition \"{desired_partition}\" marked as not ready but list of events {events} comprehensively covers this partition..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2))),
    events=st.just([
        # This partition sais that source has been reseted and is not ready right now
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1, 0), dt.datetime(2000, 1, 1, 16), False),
            created_date=dt.datetime(2000, 1, 1, 20, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1, 16), dt.datetime(2000, 1, 2), False),
            created_date=dt.datetime(2000, 1, 2, 0, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1, 16), dt.datetime(2000, 1, 2), True),
            created_date=dt.datetime(2000, 1, 2, 1, 0)
        ),
    ])
)
def test_partitioned_source_if_not_all_events_are_ready(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test decision-maker for case when we specified many partitions and one of partitions is returned as not ready as last event.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    state = prc.is_desired_partition_ready(desired_partition, events)
    assert state is False, f"Desired partition \"{desired_partition}\" marked as ready but list of events {events} doesn't cover whole partition..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2001, 1, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2001, 2, 1), False),
            created_date=dt.datetime(2001, 2, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1999, 1, 1), dt.datetime(2001, 2, 1), True),
            created_date=dt.datetime(2001, 2, 1, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1999, 1, 1), dt.datetime(2002, 1, 1), False),
            created_date=dt.datetime(2002, 1, 2)
        ),
    ])
)
def test_not_partitioned_source_if_last_event_is_not_ready(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test decision-maker if we have not partitioned source and desired partition is comprehesively covered by some interval,
    but the last event sais partition is not ready.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is False, \
        f"Desired partition \"{desired_partition}\" for NOT_PARTITIONED source marked ready but within the list of events {events}" \
        "last event is not ready and partition shouldn't be ready..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2001, 1, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 5, 1), False),
            created_date=dt.datetime(2000, 5, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 5, 1), True),
            created_date=dt.datetime(2000, 5, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 5, 1), dt.datetime(2000, 12, 31), True),
            created_date=dt.datetime(2001, 12, 31, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 5, 1), dt.datetime(2000, 12, 31), False),
            created_date=dt.datetime(2000, 12, 31, 2)
        ),
    ])
)
def test_not_partitioned_source_if_there_are_no_events_that_cover_interval(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test decision-maker if there are no Partition Registry events that cover desired partition.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is False, \
        f"Desired partition \"{desired_partition}\" marked ready but list of events {events} doesn't contain " \
        "events that can comprehensively cover desired partition..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2001, 1, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1990, 1, 1), dt.datetime(2002, 1, 1), False),
            created_date=dt.datetime(2002, 1, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1990, 1, 1), dt.datetime(2002, 1, 1), True),
            created_date=dt.datetime(2002, 1, 3)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1990, 1, 1), dt.datetime(2001, 5, 1), False),
            created_date=dt.datetime(2001, 5, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(1990, 1, 1), dt.datetime(2001, 5, 1), True),
            created_date=dt.datetime(2001, 5, 2)
        ),
    ])
)
def test_not_partitioned_source_covered_by_intersected_intervals(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test partitioned source which is comprehensively covered by intersected intervals.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is True, \
        f"Desired partition \"{desired_partition}\" marked nor ready but partition " \
        f"registry events {events} comprehensively cover desired partition..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2000, 1, 1), dt.datetime(2001, 1, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 5, 1), False),
            created_date=dt.datetime(2000, 5, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 5, 1), True),
            created_date=dt.datetime(2000, 5, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 2, 1), dt.datetime(2000, 12, 31), False),
            created_date=dt.datetime(2000, 12, 31, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 2, 1), dt.datetime(2000, 12, 31), True),
            created_date=dt.datetime(2000, 12, 31, 2)
        ),
    ])
)
def test_partitioned_source_not_covered_by_intersected_intervals(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test partitioned source when desired partition is not covered by events from Partition Registry and also these events are intersected.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is False, \
        f"Desired partition \"{desired_partition}\" marked ready but " \
        f"list of events {events} doesn't comprehensively cover desired partition..."


@given(
    desired_partition=st.just(DesiredPartition(dt.datetime(2001, 5, 1), dt.datetime(2001, 12, 1))),
    events=st.just([
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2002, 1, 1), False),
            created_date=dt.datetime(2002, 1, 1, 1)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2000, 1, 1), dt.datetime(2002, 1, 1), True),
            created_date=dt.datetime(2002, 1, 2)
        ),
        PartitionRegistryEvent(
            SourcePartition(dt.datetime(2001, 6, 1), dt.datetime(2001, 12, 31), False),
            created_date=dt.datetime(2002, 12, 1)
        ),
    ])
)
def test_partitioned_source_with_not_ready_partition_within_ready_partition(
    desired_partition: DesiredPartition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test partitioned source when desired partition is not covered by events from Partition Registry and also these events are intersected.
    """
    engine = MagicMock()
    prc = PartitionRegistryController(engine)
    assert prc.is_desired_partition_ready(desired_partition, events) is False, \
        f"Desired partition \"{desired_partition}\" marked ready but " \
        f"list of events {events} doesn't comprehensively cover desired partition..."
