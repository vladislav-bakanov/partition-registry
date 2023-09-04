import hypothesis.strategies as st
from hypothesis import given

from typing import Union

from partition_registry.data.source import BigQuerySource
from partition_registry.data.source import PostgreSQLSource
from partition_registry.data.source import AirflowDAGSource

from tests.arbitrary.source import arb_incorrect_bigquery_source
from tests.arbitrary.source import arb_incorrect_postgresql_source

from partition_registry.data.exceptions import IncorrectSourceNameError


@given(
    source=st.one_of([arb_incorrect_bigquery_source, arb_incorrect_postgresql_source]),
)
def test_1(
    source: Union[BigQuerySource, PostgreSQLSource, AirflowDAGSource]
) -> None:
    """
    Test sources with incorrect name validation
    """
    try:
        source.validate()
        assert False, f"Source \"{str(source.source_name)}\" has been successfully initialized with incorrect parameters..."
    except IncorrectSourceNameError:
        pass
