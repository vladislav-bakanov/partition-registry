import datetime as dt

from sqlalchemy import TEXT
from sqlalchemy import DATETIME
from sqlalchemy import BOOLEAN
from sqlalchemy import INTEGER

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase): ...


class PartitionRegistryORM(Base):
    __tablename__ = "partition_registry"

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True)
    source: Mapped[str] = mapped_column(TEXT, nullable=False)
    provider: Mapped[str] = mapped_column(TEXT, nullable=False)
    locked: Mapped[bool] = mapped_column(BOOLEAN, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)
    locked_at: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)
    unlocked_at: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=True)
