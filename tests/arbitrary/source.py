"Source generative properties"

import hypothesis.strategies as st

arbitrary_source_name = st.sampled_from([
    'some_source',
    'source',
    'CamelCaseSource',
])


arbitrary_bad_source_name = st.sampled_from([
    ' ',
    'Source with spaces',
    'SOME SOURCE WITH SPACES IN CAPITAL CASE',
    None
])
