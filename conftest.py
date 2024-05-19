import pytest
from lib.Utils import get_spark_session

@pytest.fixture
def spark():
    '''create a spark sesion'''
    spark_session= get_spark_session("LOCAL")
    yield spark_session
    spark_session.stop()


@pytest.fixture
def expected_results(spark):
    '''gives expected results'''
    result_schema= "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(result_schema) \
        .load("data/test_results/state_aggregate.csv")


    