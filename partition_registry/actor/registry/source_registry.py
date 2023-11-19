import datetime as dt
import pytz
from typing import Optional

import uuid

from sqlalchemy.orm import Session

from partition_registry.orm import SourceRegistryORM

from partition_registry.data.registry import Registry
from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource
from partition_registry.data.status import Status
from partition_registry.data.status import FailedRegistration


class SourceRegistry(Registry[SimpleSource, Status]):
    def __init__(
        self,
        # session: Session,
        table: type[SourceRegistryORM]
    ) -> None:
        self.table = table
        # self.session = session
        self.cache: dict[SimpleSource, RegisteredSource] = dict()

    def register(self, source: SimpleSource) -> RegisteredSource | FailedRegistration:
        if self.is_registered(source):
            return FailedRegistration("Source already registered... Ask for access_token from source owner")

        access_token = AccessToken(str(uuid.uuid4()))
        self.cache[source] = RegisteredSource(source.name, dt.datetime.now(pytz.UTC), access_token)
        return self.cache[source]
    
    def safe_register(self, source: SimpleSource) -> RegisteredSource:
        if self.is_registered(source):
            self.cache[source]

        access_token = AccessToken(str(uuid.uuid4()))
        self.cache[source] = RegisteredSource(source.name, dt.datetime.now(pytz.UTC), access_token)
        return self.cache[source]

    def is_registered(self, source: SimpleSource) -> bool:
        return source in self.cache
 
    def get_registered_source(self, source: SimpleSource) -> Optional[RegisteredSource]:
        if not self.is_registered(source):
            return None
        return self.cache[source]
