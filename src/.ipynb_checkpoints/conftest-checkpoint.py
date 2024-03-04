import pytest

from pyspark.sql import SparkSession


@pytest.fixture
def spark(request) -> SparkSession:
    """Returns a new local SparkSession for the currently running test request."""
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName(request.node.name) \
        .getOrCreate()

    # Must cleanup spark so that each test gets a newly created SparkSession.
    try:
        yield spark
    finally:
        spark.stop()
