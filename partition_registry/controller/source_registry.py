import datetime as dt

from typing import Union
from typing import Type

from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PentahoSource
from partition_registry.data.source import AirflowDAGSource
from partition_registry.data.exceptions import NotRegisteredSourceError

from partition_registry.orm.source_registry import SourceRegistry


class SourceRegistryController:
    def __init__(
        self,
        engine: Engine,
        table: Type[SourceRegistry] = SourceRegistry
    ) -> None:
        self.engine = engine
        self.session = Session(self.engine)
        self.table = table

    def is_source_registered(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
    ) -> bool:
        """Returns ss source registered or not.

        Args:
            source (Union[BigQuerySource, PentahoSource, AirflowDAGSource]): Source
        """
        record = (
            self.session
            .query(self.table)
            .filter(self.table.name == source.name)
            .filter(self.table.entity_type == str(source.entity_type))
            .filter(self.table.is_partitioned == source.is_partitioned)
            .first()
        )
        return record is not None

    def register_source(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource]
    ) -> None:
        """Register source in Source Registry

        Args:
            source (Union[BigQuerySource, PentahoSource, AirflowDAGSource]): _description_
        """
        if self.is_source_registered(source):
            return

        entry = SourceRegistry(
            name=source.name,
            entity_type=str(source.entity_type),
            is_partitioned=source.is_partitioned,
        )
        self.session.add(entry)
        self.session.commit()

    def ensure_source_registered(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource]
    ) -> None:
        """Ensure source is registered

        Args:
            source (Union[BigQuerySource, PentahoSource, AirflowDAGSource]): Source

        Raises:
            NotRegisteredSourceError: _description_
        """
        if not self.is_source_registered(source):
            self.register_source(source)
        if not self.is_source_registered(source):
            raise NotRegisteredSourceError(f"Source {source} is not registered")

    def get_source_id(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
    ) -> int:
        """Returns Source ID for specified source from Source Registry.

        Raises:
            NotRegisteredSourceError:
                It's trying to register source once, but on second unsuccessful check it raises error
        """
        self.ensure_source_registered(source)

        record = (
            self.session
            .query(self.table)
            .filter(self.table.name == source.name)
            .filter(self.table.entity_type == str(source.entity_type))
            .first()
        )
        if record is None:
            raise NotRegisteredSourceError(f"Source {source} is not registered")
        return record.id

    def get_created_at(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
    ) -> dt.datetime:
        """Returns source created date

        Raises:
            NotRegisteredSourceError: In case if we didn't find any record by specified source.
        """
        self.ensure_source_registered(source)
        record = (
            self.session
            .query(self.table)
            .filter(self.table.name == source.name)
            .filter(self.table.entity_type == str(source.entity_type))
            .first()
        )
        if record is None:
            raise NotRegisteredSourceError(f"Source {source} is not registered")
        return record.created_date

    def reset_time(self, ts: dt.datetime) -> dt.datetime:
        """Reset time to 00:00:00 the same day"""
        return ts.replace(hour=0, minute=0, second=0, microsecond=0)
