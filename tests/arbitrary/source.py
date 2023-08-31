import hypothesis.strategies as st

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PentahoSource
from partition_registry.data.source import AirflowDAGSource

from tests.arbitrary.provider import arb_provider
from tests.arbitrary.partition_strategy import arb_partitioned
from tests.arbitrary.partition_strategy import arb_not_partitioned


arb_correct_bigquery_name = st.sampled_from([
    'prodcloudna-de-production.marketing.trials_hub',
    'prodcloudna-de-production.marketing.tr_registrations',
])

arb_broken_bigquery_name = st.sampled_from([
    'prodcloudna-de-production.marketing',
    'marketing.trials_hub',
    '.marketing.trials_hub',
    '...',
    'production.marketing-.trials_hub',
])

arb_correct_pentaho_name = st.sampled_from([
    'public.trials_hub',
    'presentation.my_table'
])

arb_broken_pentaho_name = st.sampled_from([
    '.table',
    '-production.table',
    'abc',
    'table-'
    'prodcloudna-de-production.marketing.trials_hub',
])

arb_correct_airflow_dag_name = st.sampled_from([
    'trials_hub_v2',
    'tr_registration_etl',
    'wm_emails_sources_with_ga'
])

arb_bigquery_source = st.builds(
    BigQuerySource,
    name=arb_correct_bigquery_name,
    partition_strategy=st.one_of([arb_partitioned, arb_not_partitioned]),
    provider=arb_provider
)

arb_pentaho_source = st.builds(
    PentahoSource,
    name=arb_correct_pentaho_name,
    partition_strategy=st.one_of([arb_partitioned, arb_not_partitioned]),
    provider=arb_provider
)

arb_airflow_dag_source = st.builds(
    AirflowDAGSource,
    name=arb_correct_airflow_dag_name,
    partition_strategy=st.one_of([arb_partitioned, arb_not_partitioned]),
    provider=arb_provider
)

arb_partitioned_bigquery_source = st.builds(BigQuerySource, name=arb_correct_bigquery_name, partition_strategy=arb_partitioned, provider=arb_provider)
arb_partitioned_pentaho_source = st.builds(PentahoSource, name=arb_correct_pentaho_name, partition_strategy=arb_partitioned, provider=arb_provider)
arb_partitioned_ariflow_dag_source = st.builds(AirflowDAGSource, name=arb_correct_airflow_dag_name, partition_strategy=arb_partitioned, provider=arb_provider)

arb_not_partitioned_bigquery_source = st.builds(BigQuerySource, name=arb_correct_bigquery_name, partition_strategy=arb_not_partitioned, provider=arb_provider)
arb_not_partitioned_pentaho_source = st.builds(PentahoSource, name=arb_correct_pentaho_name, partition_strategy=arb_not_partitioned, provider=arb_provider)
arb_not_partitioned_ariflow_dag_source = st.builds(AirflowDAGSource, name=arb_correct_airflow_dag_name, partition_strategy=arb_not_partitioned, provider=arb_provider)
