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
