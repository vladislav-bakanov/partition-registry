import uuid

from sqlalchemy.orm import Session

# ORM
from partition_registry.orm import ProviderRegistryORM

# Models
from partition_registry.data.registry import Registry
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.access_token import AccessToken
from partition_registry.data.provider import RegisteredProvider


class ProviderRegistry(Registry):
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

    def is_registered(
        self,
        provider: SimpleProvider
    ) -> bool:
        return provider in self.cache
