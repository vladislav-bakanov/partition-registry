import datetime as dt

from hypothesis import given
from hypothesis import assume
from unittest.mock import MagicMock

from tests.arbitrary.source import arbitrary_source_name
from tests.arbitrary.provider import arbitrary_provider_name
from tests.arbitrary.access_token import arbitrary_string_token
from tests.arbitrary._datetime import arbitrary_datetime

from partition_registry.data.source import RegisteredSource
from partition_registry.data.partition import LockedPartition
from partition_registry.data.access_token import AccessToken

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.status import SuccededLock

from partition_registry.action import lock_partition


@given(
    source_name=arbitrary_source_name,
    provider_name=arbitrary_provider_name,
    access_token=arbitrary_string_token,
    partititon_created_at=arbitrary_datetime,
    start=arbitrary_datetime,
    end=arbitrary_datetime,
)
def test__lock_partition(
    source_name: str,
    provider_name: str,
    access_token: str,
    partititon_created_at: dt.datetime,
    start: dt.datetime,
    end: dt.datetime
) -> None:
    
    assume(start < end)

    provider_registry = ProviderRegistry(MagicMock())
    partition_registry = PartitionRegistry(MagicMock())
    source_registry: SourceRegistry = MagicMock()
    
    registered_source = RegisteredSource(source_name, partititon_created_at, AccessToken(access_token))
    source_registry.find_registered_source.return_value = registered_source
    
    response = lock_partition(source_name, provider_name, access_token, start, end, partition_registry, provider_registry, source_registry)

    assert isinstance(response, SuccededLock), \
        f"Expected succeded lock, got: {response}..."

    assert isinstance(response.locked_object, LockedPartition), \
        f"Expected instance of LockedPartition, got: {type(response.locked_object)}"
    
    assert response.locked_object.start == start and response.locked_object.end == end, \
        "Expected that partition start and end are the same as the start and the end on input..."
