from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration

def register_source(
    source_name: str,
    source_registry: SourceRegistry
) -> SuccededRegistration | FailedRegistration:
    """Register source within the Source Registry"""
    simple_source = SimpleSource(source_name)
    simple_source.validate()

    registered_source = source_registry.safe_register(simple_source)
    return SuccededRegistration(registered_source)
