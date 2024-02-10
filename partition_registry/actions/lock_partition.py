import datetime as dt

from partition_registry.actor.events_registry import EventsRegistry
from partition_registry.actor.partition_registry import PartitionRegistry
from partition_registry.actor.source_registry import SourceRegistry
from partition_registry.actor.provider_registry import ProviderRegistry

from partition_registry.data.event import RegisteredPartitionEvent
from partition_registry.data.event import EventType

from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import LookupFailed
from partition_registry.data.status import FailedPersist


def lock_partition(
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
        event_type=EventType.LOCK
    ):
        case ValidationFailed() as failed_validation:
            return FailedRegistration(failed_validation.message)
        case FailedPersist() as failed_persist:
            return FailedRegistration(failed_persist.message)
        case LookupFailed() as lookup_failed:
            return FailedRegistration(lookup_failed.message)
        case RegisteredPartitionEvent() as registered_event:
            ...

    return SuccededRegistration(registered_event)
