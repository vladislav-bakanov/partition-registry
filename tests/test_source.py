from hypothesis import given

from partition_registry.data.source import SimpleSource
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import ValidationSucceded

from tests.arbitrary.source import arbitrary_correct_source_name
from tests.arbitrary.source import arbitrary_incorrect_source_name
from tests.arbitrary.source import arbitrary_correct_owner
from tests.arbitrary.source import arbitrary_incorrect_owner


@given(
    source_name=arbitrary_incorrect_source_name,
    owner=arbitrary_correct_owner
)
def test_source_with_incorrect_name(
    source_name: str,
    owner: str,
) -> None:
    source = SimpleSource(source_name, owner)
    assert isinstance(source.safe_validate(), ValidationFailed), \
        "Expected failed validation because source.name contains inappropriate value, but validation successfully passed"


@given(
    source_name=arbitrary_correct_source_name,
    owner=arbitrary_incorrect_owner
)
def test_source_with_incorrect_owner(
    source_name: str,
    owner: str,
) -> None:
    source = SimpleSource(source_name, owner)
    assert isinstance(source.safe_validate(), ValidationFailed), \
        "Expected failed validation because source.owner contains inappropriate value, but validation successfully passed"


@given(
    source_name=arbitrary_correct_source_name,
    owner=arbitrary_correct_owner,
)
def test_correct_source_validation(
    source_name: str,
    owner: str,
) -> None:
    source = SimpleSource(source_name, owner)
    assert isinstance(source.safe_validate(), ValidationSucceded), \
        "Expected succeded validation, but validation failed"
