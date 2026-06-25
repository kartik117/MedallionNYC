import pytest

from medallion_nyc.spark_session import get_spark


@pytest.fixture(scope="session")
def spark():
    session = get_spark("medallion-nyc-tests")
    yield session
    session.stop()
