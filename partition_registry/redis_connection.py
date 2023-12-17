from partition_registry.data.func import safe_parse_datetime
from partition_registry.data.access_token import AccessToken
from partition_registry.data.configuration import Configuration
from partition_registry.data.partition import SimplePartition
from partition_registry.data.source import SimpleSource
from partition_registry.data.func import safe_parse_datetime
from partition_registry.data.provider import SimpleProvider
from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry

import redis

import datetime as dt

start = safe_parse_datetime(b'2023-12-01 00:00:00.000000+00:00')
end = safe_parse_datetime(b'2023-12-02 00:00:00.000000+00:00')

config = Configuration()
r = redis.Redis(
    host=config.redis.host,
    port=config.redis.port,
    db=config.redis.db,
    password=config.redis.password
)
partition_registry = PartitionRegistry(r)
source_registry = SourceRegistry(r)
provider_registry = ProviderRegistry(r)

simple_partition = SimplePartition(start, end)
simple_partition.validate()

simple_source = SimpleSource('redis-test')
simple_source.validate()

simple_provider = SimpleProvider('redis-test')
registered_source = source_registry.safe_register(simple_source)

token = AccessToken('91151a41-9c25-49c0-a82a-e7b8faf94df6')
if token != registered_source.access_token:
    print("Говно")

registered_provider = provider_registry.safe_register(simple_provider, token)
lookup_result = partition_registry.redis_lookup_locked(registered_source, registered_provider, simple_partition)
print(registered_source)


# p = r.hset('test:partition-registry', mapping={'registered_at': str(dt.datetime.now())})
# print(p)

# cur, k = r.hscan('test:partition-registry', match="registered_at")
# print(k)

# fmt = '%Y-%m-%d %H:%M:%S.%f'
# obj = b'124341241'
# decoded_object = obj.decode('utf-8')
# dt.datetime.strptime(decoded_object, fmt)

# a = r.hscan('source:registry:test_registration_via_redis')
a = b'2023-12-16 20:48:34.204719+00:00'
print(safe_parse_datetime(a))

# match k:
#     case {b'registered': bytes(is_registered)}:
#         print(is_registered.lower() in [b"true", b"1", b"t", b"y", b"yes"])
#     case _:
#         print(False)


# print(str(AccessToken.generate()))