import datetime as dt

import hypothesis.strategies as st
from hypothesis import given
from hypothesis import assume
import collections

from typing import List
from typing import Union

from partition_registry.data.partition import Partition
from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.event_state import EventState
from partition_registry.data.provider_type import ProviderType

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PostgreSQLSource
from partition_registry.data.source import AirflowDAGSource

from partition_registry.data.provider import Provider

from partition_registry.data.partition_registry_structure import PartitionRegistryStructure

from tests.arbitrary.source import arb_correct_bigquery_source
from tests.arbitrary.source import arb_correct_postgresql_source
from tests.arbitrary.source import arb_correct_airflow_dag_source
from tests.arbitrary.source import arb_correct_source
from tests.arbitrary.provider import arb_provider


@given(
    source=arb_correct_source,
    providers=st.lists(arb_provider),
    desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
    events=st.just([])
)
def test_1(
    source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource],
    providers: List[Provider],
    desired_partition: Partition,
    events: List[PartitionRegistryEvent]
) -> None:
    """
    Test case when imcoming events are epmty.
    We expect that partition will not be ready because if we have no events - we can't say that source is ready, hence we say, that desired partition is not ready
    """
    prds = PartitionRegistryStructure(events)
    assert prds.is_partition_ready(source, providers, desired_partition) is False, \
        f"Based on events: <{events}> partition {str(desired_partition)} should be ready, but marked as not ready"


# @given(
#     source=st.sampled_from([arb_correct_bigquery_source]),
#     providers=st.just([Provider(ProviderType.AIRFLOW_DAG)]),
#     desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             source=arb_correct_bigquery_source,
#             provider=st.builds(Provider, type=st.sampled_from([ProviderType.AIRFLOW_DAG])),
#             partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#             state=st.just(EventState.READY),
#             created_date=st.just(dt.datetime(2000, 12, 2))
#         )])
# )
# def test_2(
#     source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource],
#     providers: List[Provider],
#     desired_partition: Partition,
#     # events: List[PartitionRegistryEvent]
# ) -> None:
#     """Test case when we have one source event and it comprehensively covers desired partition.
#     We expect that desired partition will be ready.
#     """
#     print(source)
#     assert 1==0
    # assume(source == arb_correct_bigquery_source)
    # assume(len(providers) == 1)
    # assume(providers[0].type == ProviderType.AIRFLOW_DAG)
    # assume(events[0].provider == ProviderType.AIRFLOW_DAG)
    
    # assume(collections.Counter(event.provider for event in events) == collections.Counter(providers))

    # prds = PartitionRegistryStructure(events)
    # assert prds.is_partition_ready(source, providers, desired_partition) is True, \
    #     f"Based on events <{events}> partition {str(desired_partition)} should be ready, but marked as not ready"


# @given(
#     desired_partition=st.just(Partition(arb_correct_bigquery_source, dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(arb_correct_bigquery_source, dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1)),
#             EventState.READY,
#             dt.datetime(2000, 12, 2)
#         )
#     ])
# )
# def test_3(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     """Test case when partition is not comprehensively covered.
#     We expect that desired partition will not be ready, because part of partition is not covered.
#     """
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


# @given(
#     desired_partition=st.just(Partition(arb_correct_bigquery_source, dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(arb_correct_bigquery_source, dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2000, 12, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(arb_correct_bigquery_source, dt.datetime(2000, 1, 1), dt.datetime(2000, 10, 1)),
#             EventState.READY,
#             dt.datetime(2000, 12, 3)
#         ),
#         PartitionRegistryEvent(
#             Partition(arb_correct_bigquery_source, dt.datetime(1900, 1, 1), dt.datetime(2000, 9, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2001, 1, 1)
#         )
#     ])
# )
# def test_4(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     """Test case when there is event which doesn't cover desired partition.
#     """
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


# @given(
#     desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
#             EventState.NOT_READY,
#             dt.datetime(2001, 1, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2001, 1, 5)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 12, 31)),
#             EventState.READY,
#             dt.datetime(2001, 1, 10)
#         )
#     ])
# )
# def test_interval_tree_when_partitioned_was_ready_by_intersected_interval(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


# @given(
#     desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2000, 12, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
#             EventState.READY,
#             dt.datetime(2001, 1, 1)
#         )
#     ])
# )
# def test_interval_tree_when_partitioned_was_not_ready_because_of_partially_cover_intersected_interval(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"
        

# @given(
#     desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2)),
#             EventState.NOT_READY,
#             dt.datetime(2000, 1, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 2)),
#             EventState.READY,
#             dt.datetime(2000, 1, 2, 1)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2000, 11, 1)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
#             EventState.READY,
#             dt.datetime(2000, 11, 1, 1)
#         )
#     ])
# )
# def test_data_structure_partitions_ready_but_interval_not_covered(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"


# @given(
#     desired_partition=st.just(Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1))),
#     events=st.just([
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 1, 1), dt.datetime(2000, 12, 1)),
#             EventState.NOT_READY,
#             dt.datetime(2000, 12, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1900, 1, 1), dt.datetime(2000, 11, 1)),
#             EventState.READY,
#             dt.datetime(2001, 1, 1)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 11, 1), dt.datetime(2000, 11, 2)),
#             EventState.NOT_READY,
#             dt.datetime(2001, 1, 2)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(1999, 11, 1), dt.datetime(2000, 11, 2)),
#             EventState.READY,
#             dt.datetime(2001, 1, 4)
#         ),
#         PartitionRegistryEvent(
#             Partition(dt.datetime(2000, 8, 1), dt.datetime(2001, 1, 1)),
#             EventState.READY,
#             dt.datetime(2001, 1, 6)
#         )
#     ])
# )
# def test_interval_tree_with_complicated_different_in_time_partition_which_eventually_was_ready(
#     desired_partition: Partition,
#     events: List[PartitionRegistryEvent]
# ) -> None:
#     prds = PartitionRegistryStructure(events)
#     assert prds.is_partition_ready(desired_partition) is False, \
#         f"Based on events <{events}> partition {str(desired_partition)} shouldn't be ready, but marked as ready"
