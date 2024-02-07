import datetime as dt

from partition_registry.actor.registry import EventsRegistry
from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.data.partition import SimplePartition
from partition_registry.data.partition import RegisteredPartition
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.event import EventType

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration


def unlock_partition(
    start: dt.datetime,
    end: dt.datetime,
    partition_registry: PartitionRegistry,
    source_name: str,
    source_registry: SourceRegistry,
    provider_name: str,
    provider_registry: ProviderRegistry,
    events_registry: EventsRegistry,
) -> SuccededRegistration | FailedRegistration:

    simple_source = SimpleSource(source_name)
    match source_registry.lookup_registered(simple_source):
        case RegisteredSource() as registered_source: ...
        case _:
            return FailedRegistration(f"Source <<{simple_source.name}>> not registered. Please, register source first...")
    
    simple_provider = SimpleProvider(provider_name)
    match provider_registry.lookup_registered(simple_provider):
        case RegisteredProvider() as registered_provider: ...
        case _:
            return FailedRegistration(f"Provider <<{simple_provider.name}>> not registered. Please, register provider first...")
    
    simple_partition = SimplePartition(start, end)
    match partition_registry.lookup_registered(simple_partition, registered_source, registered_provider):
        case RegisteredPartition() as registered_partition: ...
        case _:
            return FailedRegistration(f"Partition <<{simple_partition}>> not registered. Please, register partition first...")

    registered_event = events_registry.safe_register(registered_partition, registered_source, registered_provider, EventType.UNLOCK)
    return SuccededRegistration(registered_event)
