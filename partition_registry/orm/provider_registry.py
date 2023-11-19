import datetime as dt

from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy import DATETIME

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase): ...

class ProviderRegistryORM(Base):
    __tablename__ = 'providers_registry'

    id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    name: Mapped[str] = mapped_column(String, nullable=False)
    source_id: Mapped[int] = mapped_column(Integer, nullable=False)
    access_key_hash: Mapped[str] = mapped_column(String, nullable=False)
    created_at: Mapped[dt.datetime] = mapped_column(DATETIME, nullable=False)
