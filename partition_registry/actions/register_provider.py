from partition_registry.actor.provider_registry import ProviderRegistry

from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import FailedPersist


def register_provider(
    provider_name: str,
    access_token: str,
    provider_registry: ProviderRegistry
) -> SuccededRegistration | FailedRegistration:
    match provider_registry.safe_register(provider_name, access_token):
        case AlreadyRegistered() as already_registered:
            return FailedRegistration(already_registered.message)
        case FailedRegistration() as failed_registration:
            return FailedRegistration(failed_registration.message)
        case ValidationFailed() as validation_failed:
            return FailedRegistration(validation_failed.message)
        case FailedPersist() as failed_persist:
            return FailedRegistration(failed_persist.message)
        case RegisteredProvider() as registered_provider:
            ...

    return SuccededRegistration(registered_provider)
