from redis import Redis

# Models
from partition_registry.data.registry import Registry
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.access_token import AccessToken
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.func import safe_parse_datetime


class ProviderRegistry(Registry[SimpleProvider, RegisteredProvider]):
    def __init__(self, redis: Redis) -> None:
        self.cache: dict[SimpleProvider, RegisteredProvider] = dict()
        self.redis = redis
        self.redis_path = 'registry:provider'
    
    def safe_register(
        self,
        provider: SimpleProvider,
        access_token: AccessToken
    ) -> RegisteredProvider:
        if self.is_registered(provider):
            return self.provider_memory_lookup(provider) or self.provider_redis_lookup(provider)

        registered_provider = RegisteredProvider(provider.name, access_token)

        added_records = self.redis.hset(
            f"{self.redis_path}:{provider.name}",
            mapping={
                'access_token': registered_provider.access_token.token,
                'registered_at': str(registered_provider.registered_at),
            }
        )
        # We add 2 keys and expect to receive 2 as well as a result of Redis insertion
        if added_records != 2:
            raise ValueError(f"Couldn't added provider {registered_provider} into Redis cache...")

        self.cache[provider] = registered_provider
        return registered_provider

    def is_registered(self, provider: SimpleProvider) -> bool:
        return self.provider_memory_lookup(provider) is not None or self.provider_redis_lookup(provider) is not None
    
    def provider_memory_lookup(self, provider: SimpleProvider) -> RegisteredProvider | None:
        return self.cache.get(provider)

    def provider_redis_lookup(self, provider: SimpleProvider) -> RegisteredProvider | None:
        match self.redis.hscan(f"{self.redis_path}:{provider.name}"):
            case _, {
                b'access_token': bytes(access_token_bstring),
                b'registered_at': bytes(registered_at_bstring)
            }:
                registered_at = safe_parse_datetime(registered_at_bstring)
                access_token = AccessToken(access_token_bstring.decode('utf-8'))

                if not registered_at:
                    raise ValueError(f"Registration timestamp didn't find in Redis for the {provider}...")

                return RegisteredProvider(provider.name, access_token, registered_at)