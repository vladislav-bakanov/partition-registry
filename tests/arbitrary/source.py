"Source generative properties"

import hypothesis.strategies as st

arbitrary_correct_source_name = st.sampled_from([
    'some_source',
    'source',
    'CamelCaseSource',
])

arbitrary_incorrect_source_name = st.sampled_from([
    ' ',
    'Source with spaces',
    'SOME SOURCE WITH SPACES IN CAPITAL CASE',
])

arbitrary_correct_owner = st.sampled_from([
    'some@email.com',
    'ivan.ivanov',
    'marketing',
])

arbitrary_incorrect_owner = st.sampled_from([
    ' ',
    'wrong owner with spaces',
])


arbitrary_source_name = st.one_of(arbitrary_correct_source_name, arbitrary_incorrect_source_name)
arbitrary_owner = st.one_of(arbitrary_correct_owner, arbitrary_incorrect_owner)