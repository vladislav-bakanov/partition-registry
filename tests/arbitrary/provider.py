import hypothesis.strategies as st

from partition_registry.data.provider import Provider
from partition_registry.data.provider_type import ProviderType


arb_provider = st.builds(
    Provider,
    name=st.sampled_from([
        'trials_hub_assigns_group',
        'tr_registrations_etl',
        'manual',
        'API',
    ]),
    type=st.sampled_from([ProviderType.AIRFLOW_DAG, ProviderType.API])
)
