from hypothesis import given

from partition_registry.data.provider import SimpleProvider
from partition_registry.data.status import ValidationFailed
from partition_registry.data.status import ValidationSucceded

from tests.arbitrary.provider import arbitrary_incorrect_provider_name
from tests.arbitrary.provider import arbitrary_correct_provider_name


@given(provider_name=arbitrary_incorrect_provider_name)
def test_provider_with_incorrect_name(provider_name: str) -> None:
    provider = SimpleProvider(provider_name)
    assert isinstance(provider.safe_validate(), ValidationFailed), \
        "Expected failed validation because provider.name contains inappropriate value, but validation successfully passed"


@given(provider_name=arbitrary_correct_provider_name)
def test_source_with_incorrect_owner(provider_name: str) -> None:
    provider = SimpleProvider(provider_name)
    assert isinstance(provider.safe_validate(), ValidationSucceded), \
        "Expected succeded validation, but validation failed"
