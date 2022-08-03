"""Implements unit tests of Graphlet's spark module."""

import pytest
from pyspark.sql import SparkSession


@pytest.fixture
def spark_session(app_name="PyTest fixture SparkSession") -> SparkSession:
    """spark_session generate a SparkSession for unit tests.

    Returns
    -------
    SparkSession
        A SparkSession in a local environment
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def test_spark_session_fixture(spark_session: SparkSession) -> None:
    """test_spark_session Make sure the SparkSession is created."""

    data = [("a", "b"), ("c", "d")]
    df = spark_session.createDataFrame(data, ["x", "y"])
    assert df.count() == 2
    assert df.collect() == [("a", "b"), ("c", "d")]
