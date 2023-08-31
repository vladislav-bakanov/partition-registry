from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import Boolean

from sqlalchemy import func
from sqlalchemy.ext.declarative import declarative_base


Base = declarative_base()


class SourceRegistry(Base):  # type: ignore
    __tablename__ = 'source_registry_v2'

    id = Column("id", Integer, nullable=False, primary_key=True, autoincrement=True)
    name = Column("name", String, nullable=False)
    entity_type = Column("entity_type", String, nullable=False)
    is_partitioned = Column("is_partitioned", Boolean, nullable=False)
    created_date = Column("created_date", TIMESTAMP, nullable=False, server_default=func.now())
