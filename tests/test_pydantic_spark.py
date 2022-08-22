"""Tests for pydantic-spark schema generation."""
import typing

import pyspark.sql.types as T
from pydantic_spark.base import SparkBase  # type: ignore


class TestModel(SparkBase):
    """TestModel for SparkBase schema generation."""

    key1: str
    key2: typing.Optional[int]


def test_pydantic_spark() -> None:
    """test_pydantic_spark test PyDantic Spark's spark schema generation."""

    json_schema = TestModel.spark_schema()
    spark_schema = T.StructType.fromJson(json_schema)

    test_schema = T.StructType(
        [
            T.StructField("key1", T.StringType(), False, metadata={"parentClass": "TestModel"}),
            T.StructField("key2", T.LongType(), True, metadata={"parentClass": "TestModel"}),
        ]
    )

    assert spark_schema == test_schema
