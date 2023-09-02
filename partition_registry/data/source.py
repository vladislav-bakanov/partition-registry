import re
import dataclasses as dc
from typing import Union

from partition_registry.data.partition_strategy import PartitionStrategy
from partition_registry.data.entity_type import BigQuery
from partition_registry.data.entity_type import PostgreSQL
from partition_registry.data.entity_type import AirflowDAG
from partition_registry.data.provider import Provider

from partition_registry.data.exceptions import IncorrectSourceNameError
from partition_registry.data.exceptions import UnknownSourceError


@dc.dataclass(frozen=True)
class Source:
    """Source

    Classification by types:
        - BigQuery source
        - Pentaho source
        - Airflow DAG source

    Classification by partition:
        - PARTITIONED
        - NOT_PARTITIONED

    Raises:
        IncorrectSourceNameError:
            In case if source name is incorrect. For details, please, consider to check the tests.
        UnknownSourceError:
            If source not presented as one of allowed source types.
    """
    name: str
    partition_strategy: PartitionStrategy
    provider: Provider
    entity_type: Union[BigQuery, PostgreSQL, AirflowDAG]

    def __post_init__(self) -> None:
        if not (self.name or self.name.strip()):
            raise IncorrectSourceNameError("Source name is empty")

    @property
    def is_partitioned(self) -> bool:
        """Is source partitioned or not

        Raises:
            UnknownSourceError: in case if it's not PARTITIONED | NOT PARTITIONED source
        """
        if self.partition_strategy is PartitionStrategy.PARTITIONED:
            return True
        if self.partition_strategy is PartitionStrategy.NOT_PARTITIONED:
            return False
        raise UnknownSourceError(f"Source \"{self}\" is unknown")


@dc.dataclass(frozen=True)
class BigQuerySource(Source):
    entity_type: BigQuery = dc.field(default_factory=BigQuery)

    def __post_init__(self) -> None:
        correct_table_name_with_schema_regexp = r"^([a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+)\.[a-zA-Z0-9_]+$"
        if not re.match(correct_table_name_with_schema_regexp, self.name):
            raise IncorrectSourceNameError(
                "Incorrect source name. Expected format: PROJECT_ID.DATASET_ID.TABLE_NAME, "
                f"but given: \"{self.name}\""
            )


@dc.dataclass(frozen=True)
class PostgreSQLSource(Source):
    entity_type: PostgreSQL = dc.field(default_factory=PostgreSQL)

    def __post_init__(self) -> None:
        correct_table_name_with_schema_regexp = r"^([a-z_][a-z0-9_$]*\.)[a-z_][a-z0-9_$]*$"
        if not re.match(correct_table_name_with_schema_regexp, self.name):
            raise IncorrectSourceNameError(
                "Incorrect source name. Expected: SCHEMA.TABLE_NAME, "
                f"but given: \"{self.name}\"",
            )


@dc.dataclass(frozen=True)
class AirflowDAGSource(Source):
    entity_type: AirflowDAG = dc.field(default_factory=AirflowDAG)
