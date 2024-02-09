import datetime as dt

from sqlalchemy import TEXT
from sqlalchemy import DATETIME
from sqlalchemy import INTEGER

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase): ...


class PartitionsRegistryORM(Base):
    __tablename__ = "partitions"
    __table_args__ = {'schema': 'registry'}

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True, autoincrement=True)
    start: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)
    end: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)
    source_id: Mapped[int] = mapped_column(INTEGER, nullable=False)
    provider_id: Mapped[int] = mapped_column(INTEGER, nullable=False) 
    registered_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False, default=dt.datetime.utcnow)


class PartitionEventsORM(Base):
    __tablename__ = "events"
    __table_args__ = {'schema': 'registry'}

    partition_id: Mapped[int] = mapped_column(INTEGER, nullable=False, primary_key=True)
    event_type: Mapped[str] = mapped_column(TEXT, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False)
    registered_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False, default=dt.datetime.utcnow)
