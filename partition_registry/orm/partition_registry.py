from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import TIMESTAMP
from sqlalchemy import Boolean

from sqlalchemy import func

from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class PartitionRegistry(Base):  # type: ignore
    __tablename__ = 'partition_registry_v2'

    id = Column("id", Integer, primary_key=True, nullable=False, autoincrement=True, doc="Event ID")
    source_id = Column("source_id", Integer, nullable=False, doc="Source ID from all sources")
    created_date = Column("created_date", TIMESTAMP, nullable=False, doc="Physical date of creation of this partition in UTC", server_default=func.now())
    is_partition_ready = Column("is_partition_ready", Boolean, nullable=False, doc="Information about partition readiness")
    provider = Column("provider", String, nullable=False, doc="Provider it's the one who is making operations on this partition")
    startpoint = Column("startpoint", TIMESTAMP, nullable=False, doc="Start of interval for partition")
    endpoint = Column("endpoint", TIMESTAMP, nullable=False, doc="End of interval for partition")
