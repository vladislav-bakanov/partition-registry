from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.source import RegisteredSource
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration

def register_source(
    source_name: str,
    source_registry: SourceRegistry
) -> SuccededRegistration | FailedRegistration:
    simple_source = SimpleSource(source_name)
    registered_source = source_registry.register(simple_source)
    match registered_source:
        case RegisteredSource():
            return SuccededRegistration(registered_source)
        case fail_response:
            return FailedRegistration(fail_response.error_message)
