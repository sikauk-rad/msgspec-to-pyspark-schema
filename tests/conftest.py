from collections.abc import Iterator

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope = 'session')
def spark() -> Iterator[SparkSession]:


    """
    Provide a single local Spark session for the whole test run.

    The session is pinned to UTC and a single core so that results are deterministic
    and start-up cost is only paid once.

    Yields
    ------
    pyspark.sql.SparkSession
        A local Spark session.
    """


    spark_session = (
        SparkSession.builder
        .master('local[1]')
        .appName('msgspec_to_pyspark_schema_tests')
        .config('spark.sql.shuffle.partitions', '1')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    yield spark_session
    spark_session.stop()


def pytest_collection_modifyitems(
    config: pytest.Config,
    items: list[pytest.Item],
) -> None:


    """
    Mark every test that requests the ``spark`` fixture with ``pytest.mark.spark``.

    This allows JVM-dependent tests to be deselected with ``pytest -m "not spark"``.
    """


    for item in items:
        if 'spark' in getattr(item, 'fixturenames', ()):
            item.add_marker(pytest.mark.spark)