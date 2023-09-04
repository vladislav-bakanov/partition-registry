import enum


class SourceType(enum.Enum):
    BIGQUERY = enum.auto()
    POSTGRESQL = enum.auto()
    AIRFLOW_DAG = enum.auto()
