from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

from partition_registry.data.provider import Provider
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.access_token import AccessToken

from partition_registry.data.status import FailedPersist
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import LookupFailed

from partition_registry.orm import ProvidersRegistryORM


class ProviderRegistry:
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = ProvidersRegistryORM
        self.cache: dict[str, RegisteredProvider] = dict()
    
    def safe_register(
        self,
        provider_name: str,
        access_token: str
    ) -> RegisteredProvider | AlreadyRegistered | FailedPersist | ValidationFailed:
        simple_provider = SimpleProvider(provider_name)
        match simple_provider.safe_validate():
            case ValidationFailed() as fail:
                return fail
            
        if self.is_registered(simple_provider.name):
            return AlreadyRegistered(simple_provider)

        token = AccessToken(access_token)
        match self.persist(simple_provider.name, token):
            case RegisteredProvider() as registered_provider:
                self.cache[simple_provider.name] = registered_provider
                return registered_provider
            case FailedPersist() as fail:
                return FailedPersist(fail.message)

    def lookup_registered(self, provider_name: str) -> RegisteredProvider | LookupFailed:
        return self.memory_lookup(provider_name) or self.db_lookup(provider_name)

    def is_registered(self, provider_name: str) -> bool:
        return isinstance(self.lookup_registered(provider_name), RegisteredProvider)
    
    def memory_lookup(self, provider_name: str) -> RegisteredProvider | LookupFailed:
        return self.cache.get(
            provider_name,
            LookupFailed(f"<<{provider_name}>> not registered...")
        )

    def db_lookup(self, provider_name: str) -> RegisteredProvider | LookupFailed:
        session = self.session
        rows = (
            session
            .query(self.table)
            .filter(self.table.name == provider_name)
            .all()
        )

        for row in rows:
            token = AccessToken(row.access_token)
            return RegisteredProvider(
                provider_id=row.id,
                name=row.name,
                access_token=token,
                registered_at=row.registered_at
            )
        
        return LookupFailed(f"<<{provider_name}>> not registered...")
    
    def persist(self, name: str, access_token: AccessToken) -> RegisteredProvider | FailedPersist:
        record = ProvidersRegistryORM(name=name, access_key=access_token.token)
        session = self.session
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()

        return RegisteredProvider(
            provider_id=record.id,
            name=record.name,
            access_token=AccessToken(record.access_token),
            registered_at=record.registered_at
        )
