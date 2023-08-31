import hypothesis.strategies as st
from hypothesis import given
from hypothesis import assume

from partition_registry.data.partition import Partition
from partition_registry.data.exceptions import NotPositiveIntervalError


@given(partition=st.builds(Partition))
def test_partition_with_negative_interval(partition: Partition) -> None:
    """Test partition with not positive interval"""
    try:
        assume(partition.startpoint >= partition.endpoint)
        partition.validate()
    except NotPositiveIntervalError:
        pass
    else:
        assert False


@given(partition=st.builds(Partition))
def test_partition_with_good_interval(partition: Partition) -> None:
    """Test partition with good interval"""
    assume(partition.startpoint < partition.endpoint)
    try:
        partition.validate()
    except Exception as error:
        assert False, f"During the validation error recieved: {error}"
