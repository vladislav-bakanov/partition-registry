import re

from partition_registry.data.partition_strategy import PartitionStrategy
from partition_registry.data.source_type import SourceType

from partition_registry.data.exceptions import IncorrectSourceNameError
from partition_registry.data.exceptions import UnknownSourceError


class Source:
    """
    Source is a stateful entity, which can be ready or not ready.

    Raises:
        IncorrectSourceNameError:
            In case if source name is incorrect. For details, please, consider to check the tests.
        UnknownSourceError:
            If source not presented as one of allowed source types.
    """
    def __init__(
        self,
        partition_strategy: PartitionStrategy,
        source_type: SourceType
    ) -> None:
        self.partition_strategy = partition_strategy
        self.source_type = source_type

    @property
    def source_name(self) -> str:
        raise NotImplementedError("Property \"source_name\" should be implemented...")

    def validate(self) -> None:
        if not (self.source_name or self.source_name.strip()):
            raise IncorrectSourceNameError("Source name shouldn't be empty...")

    # def __str__(self) -> str:
    #     raise NotImplementedError("Method \"<__str__>\" should be implemented...")

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


class BigQuerySource(Source):
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        partition_strategy: PartitionStrategy,
        source_type: SourceType = SourceType.BIGQUERY
    ) -> None:
        super().__init__(partition_strategy, source_type)
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id

    @property
    def source_name(self):
        return f"{self.project_id}.{self.dataset_id}.{self.table_id}"

    def validate(self) -> None:
        super().validate()
        if not re.match(r"^([a-zA-Z0-9_-]+\.[a-zA-Z0-9_]+\.[a-zA-Z0-9_]+$)", self.source_name):
            raise IncorrectSourceNameError(
                "Incorrect source name. Expected format: PROJECT_ID.DATASET_ID.TABLE_NAME, "
                f"but given: \"{self.source_name}\""
            )


class PostgreSQLSource(Source):
    def __init__(
        self,
        schema: str,
        table_name: str,
        partition_strategy: PartitionStrategy,
        source_type: SourceType = SourceType.POSTGRESQL
    ) -> None:
        super().__init__(partition_strategy, source_type)
        self.schema = schema
        self.table_name = table_name

    @property
    def source_name(self) -> str:
        return f"{self.schema}.{self.table_name}"

    def validate(self) -> None:
        super().validate()
        if not re.match(r"^([a-z_][a-z0-9_$]*\.)[a-z_][a-z0-9_$]*$", self.source_name):
            raise IncorrectSourceNameError(
                "Incorrect source name. Expected: SCHEMA.TABLE_NAME, "
                f"but given: \"{self.source_name}\"",
            )


class AirflowDAGSource(Source):
    def __init__(
        self,
        dag_name: str,
        partition_strategy: PartitionStrategy,
        source_type: SourceType = SourceType.AIRFLOW_DAG
    ) -> None:
        super().__init__(partition_strategy, source_type)
        self.dag_name = dag_name

    @property
    def source_name(self) -> str:
        return self.dag_name
