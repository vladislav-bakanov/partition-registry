from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

from partition_registry.data.registry import Registry

from partition_registry.data.provider import Provider
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import FailedPersist
from partition_registry.data.status import SuccededPersist

from partition_registry.orm import ProvidersRegistryORM


class ProviderRegistry(Registry[SimpleProvider, RegisteredProvider]):
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = ProvidersRegistryORM
        self.cache: dict[str, RegisteredProvider] = dict()
    
    def safe_register(
        self,
        provider: SimpleProvider,
        access_token: str,
    ) -> RegisteredProvider:
        match self.lookup_registered(provider):
            case RegisteredProvider() as registered_provider:
                return registered_provider
        
        token = AccessToken(access_token)
        registered_provider = RegisteredProvider(provider.name, token)
        match self.persist(registered_provider):
            case SuccededPersist(): ...
            case fail:
                raise ValueError(fail.error_message)

        self.cache[provider.name] = registered_provider
        return registered_provider

    def lookup_registered(self, provider: Provider) -> RegisteredProvider | None:
        return self.memory_lookup(provider) or self.db_lookup(provider)

    def is_registered(self, provider: SimpleProvider) -> bool:
        return isinstance(self.lookup_registered(provider), RegisteredProvider)
    
    def memory_lookup(self, provider: Provider) -> RegisteredProvider | None:
        return self.cache.get(provider.name)

    def db_lookup(self, provider: Provider) -> RegisteredProvider | None:
        session = self.session
        rows = (
            session
            .query(self.table)
            .filter(self.table.name == provider.name)
            .all()
        )
        if len(rows) == 0:
            return None

        for row in rows:
            if row is None:
                return None

            token = AccessToken(row.access_key)
            return RegisteredProvider(row.name, token)
    
    def persist(self, provider: RegisteredProvider) -> SuccededPersist | FailedPersist:
        record = ProvidersRegistryORM(
            name=provider.name,
            access_key=provider.access_token.token
        )
        session = self.session
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
        return SuccededPersist()
