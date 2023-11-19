from hypothesis import given
from unittest.mock import MagicMock

from tests.arbitrary.source import arbitrary_source_name

from partition_registry.data.source import SimpleSource
from partition_registry.data.status import SuccededRegistration

from partition_registry.actor.registry import SourceRegistry

from partition_registry.action import register_source


@given(source_name=arbitrary_source_name)
def test__register_source(source_name: str) -> None:
    table = MagicMock()
    source_registry = SourceRegistry(table)
    response = register_source(source_name, source_registry)

    assert isinstance(response, SuccededRegistration), \
        f"Expected, that registration is succeded by the specified source"
    assert source_registry.cache.get(SimpleSource(source_name)) == response.registered_object, \
        f"Expected, that object has been successfully added into the cache"