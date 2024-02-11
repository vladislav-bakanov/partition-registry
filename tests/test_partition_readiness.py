import datetime as dt
from unittest.mock import MagicMock

from hypothesis import given
from hypothesis import assume

from partition_registry.actions.check_partition_readiness import check_partition_readiness
from partition_registry.data.status import PartitionNotReady
from partition_registry.orm import PartitionsRegistryORM

from tests.arbitrary._datetime import arbitrary_datetime_with_timezone
from tests.arbitrary.source import arbitrary_source_name


@given(
    start=arbitrary_datetime_with_timezone,
    end=arbitrary_datetime_with_timezone,
    source_name=arbitrary_source_name
)
def test_readiness_on_lack_of_registered_partitions(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str
) -> None:
    assume(start < end)

    partition_registry = MagicMock()
    partition_registry.get_filtered_partitions.return_value = []
    events_registry = MagicMock()
    events_registry.get_partition_events.return_value = []

    result = check_partition_readiness(
        start=start,
        end=end,
        source_name=source_name,
        partition_registry=partition_registry,
        events_registry=events_registry,
    )

    assert isinstance(result, PartitionNotReady), (
        "Expected to get not ready partition "
        f"because there are no registered partitions for the source: <<{source_name}>>, but got: {result}"
    )
    assert result.reason, "Expected exact reason of not ready partition"


@given(
    start=arbitrary_datetime_with_timezone,
    end=arbitrary_datetime_with_timezone,
    source_name=arbitrary_source_name,
)
def test_readiness_on_lack_of_partition_events(
    start: dt.datetime,
    end: dt.datetime,
    source_name: str
) -> None:
    assume(start < end)

    partition_registry = MagicMock()
    registered_at = MagicMock()

    # There is partition, that covered full interval: [start : end]
    partition_registry.get_filtered_partitions.return_value = [
        PartitionsRegistryORM(
            id=1,
            start=start,
            end=end,
            source_id=1,
            provider_id=1,
            registered_at=registered_at
        ),
    ]
    events_registry = MagicMock()
    events_registry.get_partition_events.return_value = []

    result = check_partition_readiness(
        start=start,
        end=end,
        source_name=source_name,
        partition_registry=partition_registry,
        events_registry=events_registry,
    )

    assert isinstance(result, PartitionNotReady), (
        "Expected to get not ready partition "
        f"because there are no registered events for the source: <<{source_name}>>, but got: {result}"
    )
    assert result.reason, "Expected exact reason of not ready partition"
