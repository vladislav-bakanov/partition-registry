import datetime as dt

from unittest.mock import MagicMock

from tests.arbitrary.access_token import AccessToken

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.source import RegisteredSource
from partition_registry.actions.is_partition_ready import is_partition_ready


def test_partition_readiness_on_empty_partitions() -> None:
    desired_partition = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10))

    source_registry = SourceRegistry()
    access_token = AccessToken.generate()
    registered_source = RegisteredSource("test_source", access_token)
    source_registry.find_registered_source = MagicMock(return_value=registered_source)

    partition_registry = PartitionRegistry()
    partition_registry.get_locked_partitions_by_source = MagicMock(return_value=set())
    partition_registry.get_unlocked_partitions_by_source = MagicMock(return_value=set())

    result = is_partition_ready(
        registered_source.name,
        desired_partition.start,
        desired_partition.end,
        source_registry,
        partition_registry
    )

    assert not result, f"Expected, that partition {desired_partition} is not ready, but got: is_ready()=={result}"


def test_partition_readiness_on_locked_interval() -> None:
    desired_partition = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10))
    locked_partitions = {
        LockedPartition(dt.datetime(2000, 1, 2), dt.datetime(2000, 1, 8)),
    }
    unlocked_partitions = {
        UnlockedPartition(dt.datetime(2000, 1, 2), dt.datetime(2000, 1, 8)),
        UnlockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10)),
    }

    source_registry = SourceRegistry()
    access_token = AccessToken.generate()
    registered_source = RegisteredSource("test_source", access_token)
    source_registry.find_registered_source = MagicMock(return_value=registered_source)

    partition_registry = PartitionRegistry()
    partition_registry.get_locked_partitions_by_source = MagicMock(return_value=locked_partitions)
    partition_registry.get_unlocked_partitions_by_source = MagicMock(return_value=unlocked_partitions)

    result = is_partition_ready(
        registered_source.name,
        desired_partition.start,
        desired_partition.end,
        source_registry,
        partition_registry
    )

    assert not result, \
        "Expected that even we have unlocked intervals that cover partition - we can't " \
        "say that it's ready due to locked partition existance"


def test_partition_readiness_on_comprehensively_covered_interval_by_one_partition() -> None:
    desired_partition = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10))
    unlocked_partitions = {
        UnlockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10)),
    }

    source_registry = SourceRegistry()
    access_token = AccessToken.generate()
    registered_source = RegisteredSource("test_source", access_token)
    source_registry.find_registered_source = MagicMock(return_value=registered_source)

    partition_registry = PartitionRegistry()
    partition_registry.get_locked_partitions_by_source = MagicMock(return_value=set())
    partition_registry.get_unlocked_partitions_by_source = MagicMock(return_value=unlocked_partitions)

    result = is_partition_ready(
        registered_source.name,
        desired_partition.start,
        desired_partition.end,
        source_registry,
        partition_registry
    )

    assert result, \
        f"Expected that partition {desired_partition} is ready, because we have enough unlocked intervals: {unlocked_partitions} " \
        f"to say that partition is ready, but got: is_ready=={result}"


def test_partition_readiness_on_comprehensively_covered_interval_by_intersected_partitions() -> None:
    desired_partition = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10))
    unlocked_partitions = {
        UnlockedPartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 7)),
        # This partition partially intersects with partition above
        UnlockedPartition(dt.datetime(2000, 1, 5), dt.datetime(2000, 1, 11)),
    }

    source_registry = SourceRegistry()
    access_token = AccessToken.generate()
    registered_source = RegisteredSource("test_source", access_token)
    source_registry.find_registered_source = MagicMock(return_value=registered_source)

    partition_registry = PartitionRegistry()
    partition_registry.get_locked_partitions_by_source = MagicMock(return_value=set())
    partition_registry.get_unlocked_partitions_by_source = MagicMock(return_value=unlocked_partitions)

    result = is_partition_ready(
        registered_source.name,
        desired_partition.start,
        desired_partition.end,
        source_registry,
        partition_registry
    )

    assert result, \
        f"Expected that partition {desired_partition} is ready, because we have enough unlocked intervals: {unlocked_partitions} " \
        f"to say that partition is ready, but got: is_ready=={result}"


def test_partition_readiness_on_partial_covered_partition() -> None:
    desired_partition = SimplePartition(dt.datetime(2000, 1, 1), dt.datetime(2000, 1, 10))
    unlocked_partitions = {
        # Partition partially covers desired interval
        UnlockedPartition(dt.datetime(2000, 1, 5), dt.datetime(2000, 1, 11)),
    }

    source_registry = SourceRegistry()
    access_token = AccessToken.generate()
    registered_source = RegisteredSource("test_source", access_token)
    source_registry.find_registered_source = MagicMock(return_value=registered_source)

    partition_registry = PartitionRegistry()
    partition_registry.get_locked_partitions_by_source = MagicMock(return_value=set())
    partition_registry.get_unlocked_partitions_by_source = MagicMock(return_value=unlocked_partitions)

    result = is_partition_ready(
        registered_source.name,
        desired_partition.start,
        desired_partition.end,
        source_registry,
        partition_registry
    )

    assert not result, \
        f"Expected that partition {desired_partition} is not ready, because " \
        "partition start date (dt.datetime(2000, 1, 5)) can't cover desired interval..."
