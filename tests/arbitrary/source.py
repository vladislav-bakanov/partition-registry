import hypothesis.strategies as st

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PostgreSQLSource
from partition_registry.data.source import AirflowDAGSource

from tests.arbitrary.partition_strategy import arb_partitioned
from tests.arbitrary.partition_strategy import arb_not_partitioned


arb_correct_bigquery_source = st.builds(
    BigQuerySource,
    project_id=st.sampled_from(['my_project', 'my-project']),
    dataset_id=st.sampled_from(['my_dataset']),
    table_id=st.sampled_from(['my_table']),
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
)

arb_incorrect_bigquery_source = st.builds(
    BigQuerySource,
    project_id=st.sampled_from(['$my_project', 'my.project']),
    dataset_id=st.sampled_from(['.', '.my_dataset.']),
    table_id=st.sampled_from(['.%my_t-able']),
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
)

arb_correct_postgresql_source = st.builds(
    PostgreSQLSource,
    schema=st.sampled_from(['public', 'presentation']),
    table_name=st.sampled_from(['my_table']),
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
)

arb_incorrect_postgresql_source = st.builds(
    PostgreSQLSource,
    schema=st.sampled_from(['%-123public', '', '.']),
    table_name=st.sampled_from(['%table#$', '-table_name', '']),
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
)

arb_correct_airflow_dag_source = st.builds(
    AirflowDAGSource,
    name=st.sampled_from('dummy_dag'),
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
)

arb_correct_source = st.sampled_from([arb_correct_bigquery_source, arb_correct_postgresql_source, arb_correct_airflow_dag_source])