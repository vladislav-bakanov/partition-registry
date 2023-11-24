import datetime as dt

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.partition import SimplePartition
from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import SuccededLock
from partition_registry.data.status import FailedLock


def lock_partition(
    source_name: str,
    provider_name: str,
    access_token: str,
    start: dt.datetime,
    end: dt.datetime,
    partition_registry: PartitionRegistry,
    provider_registry: ProviderRegistry,
    source_registry: SourceRegistry,
) -> SuccededLock | FailedLock:
    simple_partition = SimplePartition(start, end)
    simple_partition.validate()

    simple_source = SimpleSource(source_name)
    simple_provider = SimpleProvider(provider_name)

    registered_source = source_registry.find_registered_source(simple_source)
    if not registered_source:
        return FailedLock(f"{simple_source} is not registered... Register source to get access_token and provide this access_token to {simple_provider}...")

    registered_provider = provider_registry.safe_register(simple_provider, AccessToken(access_token))
    if registered_provider.access_token != registered_source.access_token:
        msg = f"{registered_provider} has no access to the {simple_source}... Please, be sure that you use proper access key for the source..."
        return FailedLock(msg)

    locked_partition = partition_registry.lock(registered_source, registered_provider, simple_partition)
    return SuccededLock(locked_partition, f"{simple_partition} has been successfully locked...")
