import logging

from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session
from sqlalchemy import select

from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.registry import Registry
from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource
from partition_registry.data.status import SuccededPersist
from partition_registry.data.status import FailedPersist


class SourceRegistry(Registry[SimpleSource, RegisteredSource]):
    def __init__(self, session: Session) -> None:
        self.session = session
        self.table = SourcesRegistryORM
        self.cache: dict[SimpleSource, RegisteredSource] = dict()

    def safe_register(self, source: SimpleSource, owner: str) -> RegisteredSource:
        match self.lookup_registered(source):
            case RegisteredSource() as registered_source:
                return registered_source

        registered_source = RegisteredSource(source.name, AccessToken.generate(), owner)
        match self.persist(registered_source):
            case SuccededPersist(): ...
            case fail:
                raise ValueError(fail.error_message)

        self.cache[source] = registered_source
        return registered_source
    
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
            token = AccessToken(row.access_key)
            return RegisteredSource(row.name, token, row.owner, row.registered_at)
    
    def persist(self, source: RegisteredSource) -> SuccededPersist | FailedPersist:
        record = SourcesRegistryORM(
            name=source.name,
            owner=source.owner,
            access_key=source.access_token.token
        )
        session = self.session
        try:
            session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")
        else:
            session.commit()
        return SuccededPersist()
