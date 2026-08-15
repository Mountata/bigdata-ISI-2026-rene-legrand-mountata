import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """SparkSession partagée pour tous les tests."""
    spark = (SparkSession.builder
             .master("local[2]")
             .appName("test-transformations")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    yield spark
    spark.stop()