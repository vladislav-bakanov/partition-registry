import datetime as dt

from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import DATETIME

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class SourceRegistryORM(Base):
    __tablename__ = "source_registry"

    id: Mapped[int] = mapped_column(Integer)
    name: Mapped[str] = mapped_column(String, primary_key=True)
    initial_provider_id: Mapped[int] = mapped_column(Integer, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)