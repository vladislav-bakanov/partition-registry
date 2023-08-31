import hypothesis.strategies as st
from hypothesis import given

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PentahoSource
from partition_registry.data.provider import Provider

from partition_registry.data.partition_strategy import PartitionStrategy

from tests.arbitrary.source import arb_broken_bigquery_name
from tests.arbitrary.source import arb_broken_pentaho_name
from tests.arbitrary.partition_strategy import arb_partitioned
from tests.arbitrary.partition_strategy import arb_not_partitioned

from tests.arbitrary.provider import arb_provider

from partition_registry.data.exceptions import IncorrectSourceNameError


@given(
    source_name=arb_broken_bigquery_name,
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
    provider=arb_provider,
)
def test_bigquery_source_with_broken_name(
    source_name: str,
    partition_strategy: PartitionStrategy,
    provider: Provider
) -> None:
    """Test bigquery source with bad Source name"""
    try:
        BigQuerySource(source_name, partition_strategy=partition_strategy, provider=provider)
    except IncorrectSourceNameError:
        assert True
    else:
        assert False


@given(
    source_name=arb_broken_pentaho_name,
    partition_strategy=st.sampled_from([arb_partitioned, arb_not_partitioned]),
    provider=arb_provider,
)
def test_pentaho_source_with_broken_name(
    source_name: str,
    partition_strategy: PartitionStrategy,
    provider: Provider
) -> None:
    """Test Pentaho source with Bad name"""
    try:
        PentahoSource(source_name, partition_strategy=partition_strategy, provider=provider)
    except IncorrectSourceNameError:
        assert True
    else:
        assert False
