from redis import Redis


from partition_registry.data.registry import Registry
from partition_registry.data.access_token import AccessToken
from partition_registry.data.source import RegisteredSource
from partition_registry.data.source import SimpleSource
from partition_registry.data.func import safe_parse_datetime


class SourceRegistry(Registry[SimpleSource, RegisteredSource]):
    def __init__(self, redis: Redis) -> None:
        self.redis = redis
        self.cache: dict[SimpleSource, RegisteredSource] = dict()
        self.redis_path = 'registry:source'

    def register(self, source: SimpleSource) -> RegisteredSource:
        return self.safe_register(source)

    def safe_register(self, source: SimpleSource) -> RegisteredSource:
        if self.is_registered(source):
            return self.source_memory_lookup(source) or self.source_redis_lookup(source)

        registered_source = RegisteredSource(source.name, AccessToken.generate())

        added_records = self.redis.hset(
            f"{self.redis_path}:{source.name}",
            mapping={
                'access_token': registered_source.access_token.token,
                'registered_at': str(registered_source.registered_at),
            }
        )

        # We add 2 keys and expect to receive 2 as well as a result of Redis insertion
        if added_records != 2:
            raise ValueError(f"Couldn't added source {registered_source} into Redis cache...")

        self.cache[source] = registered_source
        return registered_source

    def is_registered(self, source: SimpleSource) -> bool:
        return self.source_memory_lookup(source) is not None or self.source_redis_lookup(source) is not None

    def source_memory_lookup(self, source: SimpleSource) -> RegisteredSource | None:
        return self.cache.get(source)

    def source_redis_lookup(self, source: SimpleSource) -> RegisteredSource | None:
        match self.redis.hscan(f"{self.redis_path}:{source.name}"):
            case _, {
                b'access_token': bytes(access_token_bstring),
                b'registered_at': bytes(registered_at_bstring)
            }:
                registered_at = safe_parse_datetime(registered_at_bstring)
                access_token = AccessToken(access_token_bstring.decode('utf-8'))

                if not registered_at:
                    raise ValueError(f"{registered_at_bstring}, Registration timestamp didn't find in Redis for the {source}...")

                return RegisteredSource(source.name, access_token, registered_at)
