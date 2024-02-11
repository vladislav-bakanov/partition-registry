from partition_registry.actor.source_registry import SourceRegistry

from partition_registry.data.source import RegisteredSource
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import FailedPersist


def register_source(
    source_name: str,
    owner: str,
    source_registry: SourceRegistry
) -> SuccededRegistration | FailedRegistration:
    match source_registry.safe_register(source_name, owner):
        case AlreadyRegistered() as already_registered:
            return FailedRegistration(already_registered.message)
        case FailedRegistration() as failed_registration:
            return FailedRegistration(failed_registration.message)
        case FailedPersist() as failed_persist:
            return FailedRegistration(failed_persist.message)
        case ValidationFailed() as validation_failed:
            return FailedRegistration(validation_failed.message)
        case RegisteredSource() as registered_source:
            ...

    return SuccededRegistration(registered_source)
