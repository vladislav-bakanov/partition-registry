class EntityType:
    def __str__(self) -> str:
        raise NotImplementedError("Method should be implemented...")


class BigQuery(EntityType):
    def __str__(self) -> str:
        return 'BIG_QUERY'


class PostgreSQL(EntityType):
    def __str__(self) -> str:
        return 'POSTGRESQL'


class AirflowDAG(EntityType):
    def __str__(self) -> str:
        return 'AIRFLOW_DAG'
