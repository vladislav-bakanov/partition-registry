import hypothesis.strategies as st
from partition_registry.data.provider import Provider


arb_provider = st.builds(
    Provider,
    name=st.sampled_from([
        'trials_hub_assigns_group',
        'tr_registrations_etl',
        'manual',
        'API',
    ])
)
