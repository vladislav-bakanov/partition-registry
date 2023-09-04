import enum


class ProviderType(enum.Enum):
    AIRFLOW_DAG = enum.auto()
    API = enum.auto()
