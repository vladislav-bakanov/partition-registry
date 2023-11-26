import datetime as dt

# from partition_registry.data.status import SuccessStatus
# from partition_registry.data.status import FailedStatus
from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from partition_registry.data.partition import SimplePartition
from partition_registry.data.error import NOT_REGISTERED

from partition_registry.actor.registry import PartitionRegistry
from partition_registry.actor.registry import SourceRegistry
from partition_registry.actor.registry import ProviderRegistry


def is_partition_ready(
    source: str,
    start: dt.datetime,
    end: dt.datetime,
    source_registry: SourceRegistry
) -> bool:
    simple_partition = SimplePartition(start, end)
    simple_partition.validate()

    simple_source = SimpleSource(source)
    match source_registry.find_registered_source(simple_source):
        case RegisteredSource() as registered_source: ...
        case _: return False

    return False