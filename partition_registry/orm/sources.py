import datetime as dt

from sqlalchemy import TEXT
from sqlalchemy import INTEGER
from sqlalchemy import DATETIME
from sqlalchemy import func

from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase): ...


class SourcesRegistryORM(Base):
    __tablename__ = "sources"
    __table_args__ = {'schema': 'registry'}

    id: Mapped[int] = mapped_column(INTEGER, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(TEXT, unique=True)
    owner: Mapped[str] = mapped_column(TEXT, nullable=True)
    access_token: Mapped[str] = mapped_column(TEXT, nullable=False)
    registered_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False, server_default=func.now())
