"""Implements unit tests of Graphlet's spark module."""

import re
from enum import Enum

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from graphlet.etl import EntityBase


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
    """test_spark_session_fixture Make sure the SparkSession is created."""

    data = [("a", "b"), ("c", "d")]
    df = spark_session.createDataFrame(data, ["x", "y"])
    assert df.count() == 2
    assert df.collect() == [("a", "b"), ("c", "d")]


class TestType(Enum):
    """TestType A test entity type."""

    foo = "foo"
    bar = "bar"


class TestEntity(EntityBase):
    """TestEntity - Test class for EntityBase with entity_type=TestType."""

    entity_type: TestType


def test_entity_base_fields():
    """test_entity_base_fields Verify that EntityBase has its fields."""

    entity = TestEntity(entity_type="foo")
    assert entity.__fields__.keys() == {"entity_id", "entity_type"}
    assert entity.dict() == dict(entity) == {"entity_id": entity.entity_id, "entity_type": "foo"}


def test_entity_id():
    """test_entity_base_id Verify that the EntityBase auto generated entity_id is a valid UUID4."""

    # Verify that the UUID4 works
    entity = TestEntity(entity_type="bar")
    assert re.search(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", entity.entity_id)


def test_entity_base_enum():
    """test_entity_base Verify that the EntityBase types are working."""

    # Verify that the type is as expected
    entity1 = TestEntity(entity_type="foo")
    assert entity1.entity_id is not None
    assert entity1.entity_type == "foo"

    # Verify that the type is as expected
    entity2 = TestEntity(entity_type="bar")
    assert entity2.entity_id is not None
    assert entity2.entity_type == "bar"

    # Verify that the type is as expected
    try:
        TestEntity(entity_type="fail")
    except ValueError:
        pass
    else:
        pytest.fail("Type outside Enum definition failed to create a ValueError")


def test_entity_pyspark():
    """test_entity_pyspark Verify we can use the class in PySpark."""

    entity = TestEntity(entity_type="foo")
    assert StructType.fromJson(entity.spark_schema())
