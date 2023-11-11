from sqlalchemy import JSON
from sqlalchemy import String
from sqlalchemy import Integer
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import DeclarativeBase

class Base(DeclarativeBase):
    pass

class SourceRegistryORM(Base):
    __tablename__ = "source_registry"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)


r = SourceRegistryORM()
r.id