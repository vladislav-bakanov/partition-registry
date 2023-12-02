
from hypothesis import given

from tests.arbitrary.source import arbitrary_bad_source_name
from partition_registry.data.source import SimpleSource

@given(
    source_name=arbitrary_bad_source_name
)
def test_source_validation(source_name: str) -> None:
    source = SimpleSource(source_name)

    try:
        source.validate()
    except ValueError: ...

    assert True, "Expected that validation will fail on broken source name, but validation successfully passed..."
