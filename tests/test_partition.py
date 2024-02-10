import datetime as dt

from hypothesis import given
from hypothesis import assume

from partition_registry.data.partition import SimplePartition
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import ValidationSucceded

from tests.arbitrary._datetime import arbitrary_datetime_with_timezone
from tests.arbitrary._datetime import arbitrary_datetime_wo_timezone
from tests.arbitrary._datetime import arbitrary_datetime


@given(
    start=arbitrary_datetime_with_timezone,
    end=arbitrary_datetime_with_timezone,
)
def test_partition_on_equal_start_and_end(
    start: dt.datetime,
    end: dt.datetime,
) -> None:
    assume(start == end)
    partition = SimplePartition(start, end)
    assert isinstance(partition.safe_validate(), ValidationFailed), \
        "Expected failed validation because start == end, but validation successfully passed"


@given(
    start=arbitrary_datetime_with_timezone,
    end=arbitrary_datetime_with_timezone
)
def test_partition_on_end_greater_than_start(
    start: dt.datetime,
    end: dt.datetime,
) -> None:
    assume(start > end)
    partition = SimplePartition(start, end)
    assert isinstance(partition.safe_validate(), ValidationFailed), \
        "Expected failed validation because start > end, but validation successfully passed"


@given(
    start=arbitrary_datetime_wo_timezone,
    end=arbitrary_datetime_wo_timezone
)
def test_partition_on_ts_without_timezones(
    start: dt.datetime,
    end: dt.datetime,
) -> None:
    assume(start < end)
    partition = SimplePartition(start, end)
    assert isinstance(partition.safe_validate(), ValidationFailed), \
        "Expected failed validation because start and end don't have timezone info"


@given(
    start=arbitrary_datetime_with_timezone,
    end=arbitrary_datetime_with_timezone
)
def test_partition_on_valid_data(
    start: dt.datetime,
    end: dt.datetime,
) -> None:
    assume(start < end)
    partition = SimplePartition(start, end)
    result = partition.safe_validate()
    assert isinstance(partition.safe_validate(), ValidationSucceded), \
        f"Expected successfully passed validation but got: {result}"
