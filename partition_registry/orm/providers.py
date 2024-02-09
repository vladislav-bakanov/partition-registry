import datetime as dt

from sqlalchemy import TEXT
from sqlalchemy import INTEGER
from sqlalchemy import DATETIME

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase): ...

class ProvidersRegistryORM(Base):
    __tablename__ = 'providers'
    __table_args__ = {'schema': 'registry'}

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True, nullable=False, autoincrement=True)
    name: Mapped[str] = mapped_column(TEXT, unique=True)
    access_token: Mapped[str] = mapped_column(TEXT, nullable=False)
    registered_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False, default=dt.datetime.utcnow)
