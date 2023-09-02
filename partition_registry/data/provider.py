class Provider:
    def __str__(self) -> str:
        raise NotImplementedError("Method should be implemented...")


class BigQuery(Provider):
    def __str__(self) -> str:
        return 'BIG_QUERY'


class PostgreSQL(Provider):
    def __str__(self) -> str:
        return 'POSTGRESQL'


class AirflowDAG(Provider):
    def __str__(self) -> str:
        return 'AIRFLOW_DAG'
