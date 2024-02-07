import datetime as dt
from dateutil import tz

from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry
from partition_registry.actor.registry import PartitionRegistry

from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider

from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import ValidationSucceded


def register_partition(
    start: dt.datetime,
    end: dt.datetime,
    partition_registry: PartitionRegistry,
    source_name: str,
    source_registry: SourceRegistry,
    provider_name: str,
    provider_registry: ProviderRegistry
) -> SuccededRegistration | FailedRegistration:
    
    if start.tzinfo is None or start.tzinfo.utcoffset(start) is None:
        start = start.astimezone(tz.UTC)
    
    if end.tzinfo is None or end.tzinfo.utcoffset(end) is None:
        end = end.astimezone(tz.UTC)
    
    simple_partition = SimplePartition(start, end)
    
    simple_source = SimpleSource(source_name)
    match simple_source.safe_validate():
        case ValidationSucceded(): ...
        case failed_validation:
            return FailedRegistration(failed_validation.error_message)
    
    match source_registry.lookup_registered(simple_source):
        case RegisteredSource() as registered_source: ...
        case _:
            return FailedRegistration(f"Source <<{simple_source.name}>> not registered. Please, register source first...")

    simple_provider = SimpleProvider(provider_name)
    match provider_registry.lookup_registered(simple_provider):
        case RegisteredProvider() as registered_provider: ...
        case _:
            return FailedRegistration(f"Provider <<{simple_provider.name}>> not registered. Please, register provider first...")

    if registered_provider.access_token != registered_source.access_token:
        return FailedRegistration(
            f"Provider <<{registered_provider.name}>> has no access for the source <<{registered_source.name}>> "
            "due to incorrect access token. Please, be sure you use a valid access key for the source..."
        )

    registered_partition = partition_registry.safe_register(simple_partition, registered_source, registered_provider)
    return SuccededRegistration(registered_partition)
