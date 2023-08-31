from typing import Type
from typing import Union
from typing import Iterable
from typing import Set

from sqlalchemy.sql.expression import or_
from sqlalchemy.sql.expression import and_
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from partition_registry.orm.partition_registry import PartitionRegistry

from partition_registry.controller.source_registry import SourceRegistryController

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PentahoSource
from partition_registry.data.source import AirflowDAGSource

from partition_registry.data.partition import DesiredPartition
from partition_registry.data.partition import SourcePartition
from partition_registry.data.partition_strategy import PartitionStrategy
from partition_registry.data.partition_registry_event import PartitionRegistryEvent
from partition_registry.data.interval_tree import IntervalTree

from partition_registry.data.exceptions import UnknownPartitionStrategyError


class PartitionRegistryController:
    def __init__(
        self,
        engine: Engine,
        table: Type[PartitionRegistry] = PartitionRegistry,
    ) -> None:
        self.engine = engine
        self.session = Session(self.engine)
        self.table = table

    def is_desired_partition_ready(
        self,
        desired_partition: DesiredPartition,
        partition_registry_events: Iterable[PartitionRegistryEvent]
    ) -> bool:
        """
        Core function to determine is desired partition covered by other passed partitions or not

        Args:
            desired_partition (DesiredPartition): partition you want to check
            partition_registry_events (typing.Iterable[PartitionRegistryEvent]):
                Events for the same source as specified partition.

        Raises:
            UnknownPartitionStrategyError: in case if source partition strategy not in PARTITIONED | NOT_PARTITIONED
        """
        if not partition_registry_events:
            return False

        interval_tree = IntervalTree(desired_partition)
        for event in partition_registry_events:
            event.validate()
            interval_tree.add_node(event)
        return desired_partition.size_in_sec == sum(node.partition.size_in_sec for node in interval_tree.nodes if node.partition.is_ready)

    def get_source_events(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
        desired_partition: DesiredPartition
    ) -> Set[PartitionRegistryEvent]:
        """Get all unique events from db as PartitionRegistryEvents

        Args:
            source (Union[BigQuerySource, PentahoSource, AirflowDAGSource]): Source
            desired_partition (DesiredPartition): partition you want to check.

        Raises:
            UnknownPartitionStrategyError: in case if source partition strategy not in PARTITIONED | NOT_PARTITIONED
        """
        desired_partition.validate()
        src = SourceRegistryController(engine=self.engine)
        source_id = src.get_source_id(source=source)

        if source.partition_strategy == PartitionStrategy.NOT_PARTITIONED:
            raw_events = (
                self.session
                .query(self.table)
                .filter(self.table.source_id == source_id)
                .filter(self.table.provider == source.provider.name)
                .filter(self.table.startpoint <= desired_partition.startpoint)
                .filter(self.table.endpoint >= desired_partition.endpoint)
                .all()
            )
        elif source.partition_strategy == PartitionStrategy.PARTITIONED:
            raw_events = (
                self.session
                .query(self.table)
                .filter(self.table.source_id == source_id)
                .filter(self.table.provider == source.provider.name)
                .filter(
                    or_(
                        # Left tail partition
                        and_(self.table.startpoint <= desired_partition.startpoint, self.table.endpoint >= desired_partition.startpoint),
                        and_(self.table.startpoint >= desired_partition.startpoint, self.table.endpoint <= desired_partition.endpoint),
                        # Right tail partition
                        and_(self.table.startpoint <= desired_partition.endpoint, self.table.endpoint >= desired_partition.endpoint)
                    )
                )
                .all()
            )
        else:
            raise UnknownPartitionStrategyError(f"Source \"{source}\" partition strategy is unknown")

        return set(PartitionRegistryEvent(SourcePartition(event.startpoint, event.endpoint, event.is_partition_ready), event.created_date) for event in raw_events)

    def set_source_state(
        self,
        source: Union[BigQuerySource, PentahoSource, AirflowDAGSource],
        partition: SourcePartition,
    ) -> None:
        """Register source state in Partition Registry.

        Args:
            source (Union[BigQuerySource, PentahoSource, AirflowDAGSource]): Source
            partition (Union[ReadyPartition, NotReadyPartition]): partition for which you are going to set a state.
        """
        partition.validate()
        src = SourceRegistryController(self.engine)
        source_id = src.get_source_id(source)

        if source.is_partitioned:
            startpoint = partition.startpoint
        else:
            startpoint = src.get_created_at(source)

        entity = PartitionRegistry(
            source_id=source_id,
            is_partition_ready=partition.is_ready,
            provider=source.provider.name,
            startpoint=startpoint,
            endpoint=partition.endpoint
        )
        self.session.add(entity)
        self.session.commit()
