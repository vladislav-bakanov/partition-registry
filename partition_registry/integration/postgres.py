import os
import requests
from typing import Iterator
from contextlib import contextmanager

from sqlalchemy import create_engine
from sqlalchemy import Engine
from sqlalchemy.orm import Session
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session


@contextmanager
def init_postgres_engine() -> Iterator[Engine]:
    host = os.getenv('POSTGRES_APPLICATION_HOST', 'localhost')
    user = os.getenv('POSTGRES_APPLICATION_USER', 'postgres')
    password = os.getenv('POSTGRES_APPLICATION_PASSWORD', 'changeme')
    db = os.getenv('POSTGRES_APPLICATION_DATABASE_NAME', 'partition_registry')
    engine = create_engine(f'postgresql+psycopg2://{user}:{password}@{host}/{db}', echo=False)
    try:
        yield engine
    finally:
        engine.dispose()


@contextmanager
def init_postgres_session() -> Iterator[scoped_session[Session]]:
    with init_postgres_engine() as engine:
        session_factory = sessionmaker(bind=engine)
        session = scoped_session(session_factory)
        try:
            yield session
        finally:
            session.remove()