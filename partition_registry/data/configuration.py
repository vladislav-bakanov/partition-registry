from typing import Self
import dataclasses as dc
import os


@dc.dataclass(frozen=True)
class RedisConfiguration:
    host: str
    db: str
    port: str
    password: str

    @classmethod
    def parse(cls) -> Self:
        """Parse Redis configuration from the local variables
        """
        host = os.environ.get('REDIS_HOST', 'localhost')
        db = int(os.environ.get('REDIS_DB', 0))
        port = int(os.environ.get('REDIS_PORT', 6379))
        password = os.environ.get('REDIS_PASSWORD', None)

        return RedisConfiguration(host, db, port, password)


@dc.dataclass(frozen=True)
class Configuration:
    redis: RedisConfiguration = dc.field(default=RedisConfiguration.parse())
