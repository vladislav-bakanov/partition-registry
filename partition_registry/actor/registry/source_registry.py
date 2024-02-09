import logging

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy import select

from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.registry import Registry
from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource

from partition_registry.data.status import FailedPersist
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import AlreadyRegistered


class SourceRegistry(Registry[SimpleSource, RegisteredSource]):
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = SourcesRegistryORM
        self.cache: dict[SimpleSource, RegisteredSource] = dict()

    def safe_register(self, source: SimpleSource, owner: str) -> RegisteredSource | AlreadyRegistered | FailedRegistration:
        if self.is_registered(source):
            return AlreadyRegistered(source)

        match self.persist(source.name, source.owner, AccessToken.generate()):
            case RegisteredSource() as registered_source:
                self.cache[source] = registered_source
                return registered_source
            case FailedPersist() as failed_persist:
                return FailedRegistration(failed_persist.message)
    
    def lookup_registered(self, source: SimpleSource) -> RegisteredSource | None:
        return self.memory_lookup(source) or self.db_lookup(source)
    
    def is_registered(self, source: SimpleSource) -> bool:
        return isinstance(self.lookup_registered(source), RegisteredSource)

    def memory_lookup(self, source: SimpleSource) -> RegisteredSource | None:
        return self.cache.get(source)

    def db_lookup(self, source: SimpleSource) -> RegisteredSource | None:
        rows = (
            self.session
            .query(self.table)
            .filter(self.table.name == source.name)
            .all()
        )
        if not rows:
            return None

        for row in rows:
            token = AccessToken(row.access_token)
            return RegisteredSource(
                source_id=row.id,
                name=row.name,
                owner=row.owner,
                access_token=token,
                registered_at=row.registered_at
            )
    
    def persist(self, name: str, owner: str, token: AccessToken) -> RegisteredSource | FailedPersist:
        record = SourcesRegistryORM(
            name=name,
            owner=owner,
            access_token=token.token
        )
        try:
            self.session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            self.session.commit()

        return RegisteredSource(
            source_id=record.id,
            name=record.name,
            owner=record.owner,
            access_token=AccessToken(record.access_token),
            registered_at=record.registered_at
        )
