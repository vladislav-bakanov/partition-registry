import datetime as dt

from hypothesis import given
from hypothesis import assume
from unittest.mock import MagicMock

from tests.arbitrary.source import arbitrary_source_name
from tests.arbitrary.provider import arbitrary_provider_name
from tests.arbitrary.access_token import arbitrary_string_token
from tests.arbitrary._datetime import arbitrary_datetime

from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import LockedPartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.access_token import AccessToken

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.status import SuccededUnlock

from partition_registry.action import unlock_partition


@given(
    source_name=arbitrary_source_name,
    provider_name=arbitrary_provider_name,
    access_token=arbitrary_string_token,
    partititon_created_at=arbitrary_datetime,
    partititon_locked_at=arbitrary_datetime,
    start=arbitrary_datetime,
    end=arbitrary_datetime,
)
def test__unlock_partition(
    source_name: str,
    provider_name: str,
    access_token: str,
    partititon_created_at: dt.datetime,
    partititon_locked_at: dt.datetime,
    start: dt.datetime,
    end: dt.datetime
) -> None:
    
    assume(start < end)

    source_registry: SourceRegistry = MagicMock()
    registered_source = RegisteredSource(source_name, partititon_created_at, AccessToken(access_token))
    source_registry.find_registered_source = MagicMock()
    source_registry.find_registered_source.return_value = registered_source
    
    provider_registry = ProviderRegistry(MagicMock())
    partition_registry = PartitionRegistry(MagicMock())
    
    token = AccessToken(access_token)
    registered_provider = RegisteredProvider(provider_name, token)
    locked_partition = LockedPartition(start, end, partititon_created_at, partititon_locked_at)
    
    
    partition_registry.cache = MagicMock()
    partition_registry.cache = {(registered_source, registered_provider): {locked_partition, }}
    
    response = unlock_partition(source_name, provider_name, access_token, start, end, partition_registry, provider_registry, source_registry)

    assert isinstance(response, SuccededUnlock), f"Expected succeded unlock, but got: {response}"

    partition_registry_cache = [obj for obj in partition_registry.cache.get((registered_source, registered_provider))]

    assert len(partition_registry_cache) == 1, \
        f"Expected only one partition after all operations, got: {partition_registry_cache}"
    
    assert isinstance(partition_registry_cache[0], UnlockedPartition), \
        "Expected cache in Partition Registry with object of type UnlockedPartition, but got: " \
        f"{type(partition_registry_cache)}"

    assert response.unlocked_object.start == start and response.unlocked_object.end == end, \
        "Expected that unlocked object has the same start and end timestamps as on input " \
        f"{{start: {start}}} & {{end: {end}}}, but got: " \
        f"{{start: {response.unlocked_object.start}}} & {{end: {response.unlocked_object.end}}}"
