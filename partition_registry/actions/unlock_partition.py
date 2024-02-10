import datetime as dt

from partition_registry.data.func import localize

from partition_registry.actor.registry import EventsRegistry
from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

from partition_registry.data.partition import SimplePartition
from partition_registry.data.event import RegisteredPartitionEvent
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.event import EventType

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import LookupFailed
from partition_registry.data.status import FailedPersist


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

    match events_registry.safe_register(
        start=start,
        end=end,
        partition_registry=partition_registry,
        source_name=source_name,
        source_registry=source_registry,
        provider_name=provider_name,
        provider_registry=provider_registry,
        event_type=EventType.UNLOCK
    ):
        case RegisteredPartitionEvent() as registered_event:
            return SuccededRegistration(registered_event)
        case ValidationFailed() as failed_validation:
            return FailedRegistration(failed_validation.message)
        case FailedPersist() as failed_persist:
            return FailedRegistration(failed_persist.message)
        case LookupFailed() as lookup_failed:
            return FailedRegistration(lookup_failed.message)
    
    
    simple_source = SimpleSource(source_name)
    match simple_source.safe_validate():
        case ValidationFailed() as failed_validation:
            return failed_validation
    
    match source_registry.lookup_registered(simple_source):
        case None:
            return FailedRegistration(f"<<{simple_source.name}>> not registered. Please, register source first...")
        case RegisteredSource() as registered_source: ...
    
    simple_provider = SimpleProvider(provider_name)
    match simple_provider.safe_validate():
        case ValidationFailed() as failed_validation:
            return failed_validation
    
    match provider_registry.lookup_registered(simple_provider):
        case None:
            return FailedRegistration(f"<<{simple_provider.name}>> not registered. Please, register provider first...")
        case RegisteredProvider() as registered_provider: ...
    
    start = localize(start)
    end = localize(end)

    simple_partition = SimplePartition(start, end)
    match simple_partition.safe_validate():
        case ValidationFailed() as failed_validation:
            return failed_validation

    if not partition_registry.is_registered(simple_partition, registered_source, registered_provider):
        return FailedRegistration(f"Partition <<{simple_partition}>> not registered. Please, register partition first...")

    match events_registry.safe_register(
        partition=simple_partition,
        partition_registry=partition_registry,
        source=registered_source,
        provider=registered_provider,
        event_type=EventType.UNLOCK
    ):
        case RegisteredPartitionEvent() as registered_event:
            ...
        case fail:
            return fail
    
    return SuccededRegistration(registered_event)
