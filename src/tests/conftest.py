import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """A single, minimal local SparkSession shared by all tests in the run.

    local[1] + few shuffle partitions keeps the JVM startup and every test
    fast — correctness of the transformations doesn't depend on parallelism.
    """
    spark = (
        SparkSession.builder
        .appName("silver-tests")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()
