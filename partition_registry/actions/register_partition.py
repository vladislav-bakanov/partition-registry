import datetime as dt

from partition_registry.actor.source_registry import SourceRegistry
from partition_registry.actor.provider_registry import ProviderRegistry
from partition_registry.actor.partition_registry import PartitionRegistry

from partition_registry.data.partition import RegisteredPartition

from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import AccessDenied
from partition_registry.data.status import LookupFailed
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import FailedPersist
from partition_registry.data.status import ValidationFailed


def register_partition(
    start: dt.datetime,
    end: dt.datetime,
    partition_registry: PartitionRegistry,
    source_name: str,
    source_registry: SourceRegistry,
    provider_name: str,
    provider_registry: ProviderRegistry
) -> SuccededRegistration | FailedRegistration:
    match partition_registry.safe_register(
        start=start,
        end=end,
        source_name=source_name,
        source_registry=source_registry,
        provider_name=provider_name,
        provider_registry=provider_registry
    ):
        case FailedPersist() as failed_persist:
            return FailedRegistration(failed_persist.message)
        case AlreadyRegistered() as already_registered:
            return FailedRegistration(already_registered.message)
        case ValidationFailed() as validation_failed:
            return FailedRegistration(validation_failed.message)
        case LookupFailed() as lookup_failed:
            return FailedRegistration(lookup_failed.message)
        case AccessDenied() as access_denied:
            return FailedRegistration(access_denied.message)
        case RegisteredPartition() as registered_partition:
            ...

    return SuccededRegistration(registered_partition)
