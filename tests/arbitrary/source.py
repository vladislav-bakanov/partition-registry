import hypothesis.strategies as st

arbitrary_source_name = st.sampled_from([
    'some_source',
    'source',
    'CamelCaseSource',
    'Source with spaces',
    'SOME CAPITAL SOURCE WITH SPACES',
])


