import uuid
from hypothesis import strategies as st
from partition_registry.data.access_token import AccessToken

arbitrary_uuid = st.builds(uuid.UUID)
arbitrary_access_token = st.builds(AccessToken, str(arbitrary_uuid))
arbitrary_string_token = st.sampled_from([
    'qweasda-231231-asdasd-12312',
    '1234-5678-9012-1234',
    '123asdasdFdasdsaD=12141240%$####',
])