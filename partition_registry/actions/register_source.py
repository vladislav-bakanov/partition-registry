from partition_registry.actor.registry import SourceRegistry

from partition_registry.data.source import SimpleSource
from partition_registry.data.status import SuccededRegistration
from partition_registry.data.status import FailedRegistration
from partition_registry.data.status import ValidationSucceded


def register_source(
    source_name: str,
    owner: str,
    source_registry: SourceRegistry
) -> SuccededRegistration | FailedRegistration:
    simple_source = SimpleSource(source_name)
    match simple_source.safe_validate():
        case ValidationSucceded(): ...
        case failed_validation:
            return FailedRegistration(failed_validation.error_message)

    registered_source = source_registry.safe_register(simple_source, owner)
    return SuccededRegistration(registered_source)