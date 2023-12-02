"Provider generative properties"

import hypothesis.strategies as st
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from tests.arbitrary.access_token import arbitrary_access_token

arbitrary_provider_name = st.sampled_from([
    "CamelCaseProvider",
    "Provider with spaces",
    "some_provider_with_underscores",
    "provider",
    "PROVIDER",
    "Provider"
])

arbitrary_simple_provider = st.builds(SimpleProvider, arbitrary_provider_name)
arbitrary_registered_provider = st.builds(RegisteredProvider, arbitrary_provider_name, arbitrary_access_token)
