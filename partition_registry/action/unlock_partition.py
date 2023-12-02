import datetime as dt

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import UnlockedPartition
from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import SuccededUnlock
from partition_registry.data.status import FailedUnlock


def unlock_partition(
    source_name: str,
    provider_name: str,
    access_token: str,
    start: dt.datetime,
    end: dt.datetime,
    partition_registry: PartitionRegistry,
    provider_registry: ProviderRegistry,
    source_registry: SourceRegistry,
) -> SuccededUnlock | FailedUnlock:
    """
    Unlock Source partition by Provider
    
    Params:
        source_name: str - name of the source you trying to unlock
        provider_name: str - name of the provider you trying to unlock within the source
        access_token: str - access token to access the source by the provider.
            In case if provider specified incorrect source access token - function returns FailedUnlock
        start: dt.datetime - start of a Source interval to unlock
        end: dt.datetime - end of a Source interval to unlock
        partition_registry: PartitionRegistry - partition registry to manage all partitions
        source_registry: SourceRegistry - source registry to manage all sources

    Returns: SuccededUnlock | FailedUnlock
        Returns FailedUnlock in cases:
            - Source is not registered
            - Provider tried to access to source with wrong access key

        Returns SuccededUnlock if partition has been successfully locked

    """
    simple_partition = SimplePartition(start, end)
    simple_partition.validate()

    simple_source = SimpleSource(source_name)
    simple_source.validate()

    simple_provider = SimpleProvider(provider_name)
    token = AccessToken(access_token)

    registered_source = source_registry.find_registered_source(simple_source)
    if not registered_source:
        return FailedUnlock(f"{simple_source} is not registered... Register source to get access_token and provide this access_token to {simple_provider}...")

    registered_provider = provider_registry.safe_register(simple_provider, token)
    if registered_provider.access_token != registered_source.access_token:
        msg = f"{registered_provider} has no access to the {simple_source}... Please, be sure that you use proper access key for the source..."
        return FailedUnlock(msg)

    match partition_registry.unlock(registered_source, registered_provider, simple_partition):
        case FailedUnlock() as failed_unlock:
            return failed_unlock
        case UnlockedPartition() as unlocked_partition:
            return SuccededUnlock(unlocked_partition, f"{simple_partition} has been successfully locked...")
        case unknown_return_type:
            raise TypeError(f"Unexpected return type: {unknown_return_type}")
