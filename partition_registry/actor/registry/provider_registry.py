from typing import Optional

# from sqlalchemy.orm import Session

# ORM
from partition_registry.orm import ProviderRegistryORM

# Models
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.access_token import AccessToken
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.status import FailedRegistration


class ProviderRegistry:
    def __init__(
        self,
        # session: Session,
        table: type[ProviderRegistryORM]
    ) -> None:
        self.table = table
        # self.session = session
        self.cache: dict[SimpleProvider, RegisteredProvider] = dict()

    def safe_register(
        self,
        provider: SimpleProvider,
        access_token: AccessToken
    ) -> RegisteredProvider:
        if self.is_registered(provider):
            return self.cache[provider]

        self.cache[provider] = RegisteredProvider(provider.name, access_token)
        return self.cache[provider]
    
    def register(
        self,
        provider: SimpleProvider,
        access_token: AccessToken
    ) -> RegisteredProvider | FailedRegistration:
        if self.is_registered(provider):
            return FailedRegistration(f"{provider} already registered...")

        self.cache[provider] = RegisteredProvider(provider.name, access_token)
        return self.cache[provider]


    def is_registered(
        self,
        provider: SimpleProvider
    ) -> bool:
        return provider in self.cache

    def find_registered_provider(self, provider: SimpleProvider) -> Optional[RegisteredProvider]:
        return self.cache.get(provider)