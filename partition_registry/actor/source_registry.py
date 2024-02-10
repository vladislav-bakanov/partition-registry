from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import Session

from partition_registry.orm import SourcesRegistryORM

from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource

from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import FailedPersist
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import AlreadyRegistered
from partition_registry.data.status import LookupFailed


class SourceRegistry:
    def __init__(self, session: scoped_session[Session]) -> None:
        self.session = session
        self.table = SourcesRegistryORM
        self.cache: dict[str, RegisteredSource] = dict()

    def safe_register(
        self,
        source_name: str,
        owner: str
    ) -> RegisteredSource | AlreadyRegistered | FailedRegistration | ValidationFailed | FailedPersist:
        simple_source = SimpleSource(source_name, owner)
        match simple_source.safe_validate():
            case ValidationFailed() as failed_validation:
                return ValidationFailed(failed_validation.message)

        if self.is_registered(simple_source.name):
            return AlreadyRegistered(simple_source)

        match self.persist(simple_source, AccessToken.generate()):
            case RegisteredSource() as registered_source:
                self.cache[simple_source.name] = registered_source
            case FailedPersist() as failed_persist:
                return failed_persist

        return registered_source

    def lookup_registered(self, source_name: str) -> RegisteredSource | LookupFailed:
        return (
            self.memory_lookup(source_name)
            or self.db_lookup(source_name)
            or LookupFailed(f"Source<<{source_name}>> not registered...")
        )

    def is_registered(self, source_name: str) -> bool:
        return isinstance(self.lookup_registered(source_name), RegisteredSource)

    def memory_lookup(self, source_name: str) -> RegisteredSource | None:
        return self.cache.get(source_name)

    def db_lookup(self, source_name: str) -> RegisteredSource | None:
        rows = (
            self.session
            .query(self.table)
            .filter(self.table.name == source_name)
            .all()
        )

        for row in rows:
            token = AccessToken(row.access_token)
            return RegisteredSource(
                source_id=row.id,
                name=row.name,
                owner=row.owner,
                access_token=token,
                registered_at=row.registered_at
            )

        return None

    def persist(self, source: SimpleSource, access_token: AccessToken) -> RegisteredSource | FailedPersist:
        record = SourcesRegistryORM(
            name=source.name,
            owner=source.owner,
            access_token=access_token.token
        )
        try:
            self.session.add(record)
        except Exception as e:
            return FailedPersist(f"Persist failed with error: {e}")

        self.session.commit()

        return RegisteredSource(
            source_id=record.id,
            name=record.name,
            owner=record.owner,
            access_token=AccessToken(record.access_token),
            registered_at=record.registered_at
        )
