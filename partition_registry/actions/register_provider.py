from partition_registry.actor.registry import ProviderRegistry

from partition_registry.data.provider import SimpleProvider
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration


def register_provider(
    provider_name: str,
    access_token: str,
    provider_registry: ProviderRegistry
) -> SuccededRegistration | FailedRegistration:
    simple_provider = SimpleProvider(provider_name)
    registered_provider = provider_registry.safe_register(simple_provider, access_token)
    return SuccededRegistration(registered_provider)
