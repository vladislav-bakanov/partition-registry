"Provider generative properties"

import hypothesis.strategies as st
from partition_registry.data.provider import SimpleProvider
from partition_registry.data.provider import RegisteredProvider
from tests.arbitrary.access_token import arbitrary_access_token

arbitrary_correct_provider_name = st.sampled_from([
    "CamelCaseProvider",
    "some_provider_with_underscores",
    "provider",
    "PROVIDER",
    "Provider"
])

arbitrary_incorrect_provider_name = st.sampled_from([
    "wrong provider name"
])

arbitrary_correct_simple_provider = st.builds(SimpleProvider, arbitrary_correct_provider_name)
arbitrary_incorrect_simple_provider = st.builds(SimpleProvider, arbitrary_incorrect_provider_name)
arbitrary_simple_provider = st.one_of([arbitrary_correct_simple_provider, arbitrary_incorrect_simple_provider])
