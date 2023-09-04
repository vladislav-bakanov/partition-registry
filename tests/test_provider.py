import hypothesis.strategies as st
from hypothesis import given

from partition_registry.data.provider import Provider
from partition_registry.data.provider_type import ProviderType
from partition_registry.data.exceptions import IncorrectProviderNameError


@given(
    provider=st.one_of([
        st.builds(
            Provider,
            name=st.sampled_from(['', ' ', '  ']),
            provider_type=st.sampled_from([ProviderType])
        )
    ])
)
def test_1(provider: Provider) -> None:
    """Test Provider initialization with incorrect name"""
    try:
        provider.validate
        assert False, "Provider name is empty and provider has successfully passed all validations, but shouldn't be..."
    except IncorrectProviderNameError:
        pass
