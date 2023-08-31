import hypothesis.strategies as st
from hypothesis import given

from partition_registry.data.provider import Provider
from partition_registry.data.exceptions import IncorrectProviderNameError


@given(name=st.sampled_from(['', ' ', '  ']))
def test_incorrect_provider_name(name: str) -> None:
    """Test Provider initialization with incorrect name"""
    try:
        Provider(name)
    except IncorrectProviderNameError:
        pass
    else:
        assert False, "Provider name is empty, but it has been successfully initialized, but shouldn't be..."
