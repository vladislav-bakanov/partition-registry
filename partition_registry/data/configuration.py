import dataclasses as dc


@dc.dataclass(frozen=True)
class Configuration:
    pg_connection: str
    persist_size: int
