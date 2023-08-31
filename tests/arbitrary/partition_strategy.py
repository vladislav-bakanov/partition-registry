import hypothesis.strategies as st

from partition_registry.data.partition_strategy import PartitionStrategy

arb_partitioned = st.just(PartitionStrategy.PARTITIONED)
arb_not_partitioned = st.just(PartitionStrategy.NOT_PARTITIONED)
