import datetime as dt

from sqlalchemy import TEXT
from sqlalchemy import DATETIME
from sqlalchemy import INTEGER

from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column


class Base(DeclarativeBase): ...

class PartitionEventsORM(Base):
    __tablename__ = "events"
    __table_args__ = {'schema': 'registry'}

    partition_id: Mapped[int] = mapped_column(INTEGER, nullable=False, primary_key=True)
    event_type: Mapped[str] = mapped_column(TEXT, nullable=False)
    registered_at: Mapped[dt.datetime] = mapped_column(DATETIME(timezone=True), nullable=False, default=dt.datetime.utcnow)
