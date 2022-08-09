"""Implements unit tests of Graphlet's spark module."""

import re

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from graphlet.etl import EdgeBase, EntityBase, NodeBase


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


class TestEntity(EntityBase):
    """TestEntity - Test entity class for EntityBase with entity_type="test_entity"."""

    entity_type: str = "test_entity"


def test_entity_base_fields() -> None:
    """test_entity_base_fields Verify that EntityBase has its fields."""

    entity = TestEntity()
    assert entity.__fields__.keys() == {"entity_id", "entity_type"}
    assert entity.dict() == dict(entity) == {"entity_id": entity.entity_id, "entity_type": "test_entity"}


def test_entity_id() -> None:
    """test_entity_base_id Verify that the EntityBase auto generated entity_id is a valid UUID4."""

    # Verify that the UUID4 works
    entity = TestEntity()
    assert re.search(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", entity.entity_id)


def test_entity_base_enum() -> None:
    """test_entity_base Verify that the EntityBase types are working."""

    # Verify that the type is as expected
    entity1 = TestEntity()
    assert entity1.entity_id is not None
    assert entity1.entity_type == "test_entity"

    # Verify that the type is as expected
    try:
        TestEntity(entity_type=None)
    except ValueError:
        pytest.fail("Set null entity type")


def test_entity_pyspark() -> None:
    """test_entity_pyspark Verify we can use the class in PySpark."""

    entity = TestEntity()
    assert StructType.fromJson(entity.spark_schema())


class Movie(NodeBase):
    """Movie genres."""

    entity_type = "movie"
    genre: str


def test_node() -> None:
    """test_node Test the EdgeBase class."""

    comedy_node = Movie(genre="comedy")
    assert comedy_node.genre == "comedy"


class Director(NodeBase):
    """A person in hollywood."""

    entity_type = "director"
    name: str


class Directed(EdgeBase):
    """A director directed a movie."""

    entity_type = "directed"


def test_nodes_edge() -> None:
    """test_edge Test the Relationship class."""

    comedy_node = Movie(genre="comedy")
    director_node = Director(name="Dario Argento")
    director_edge = Directed(src=director_node.entity_id, dst=comedy_node.entity_id, entity_type="director")

    # Test ze nodes
    assert comedy_node.genre == "comedy"
    assert director_node.name == "Dario Argento"

    # Test ze edge
    assert director_edge.src == director_node.entity_id
    assert director_edge.dst == comedy_node.entity_id


class NoType(EntityBase):
    """An entity without a specified type."""

    pass


def test_default_entity_type() -> None:
    """Verify that without a entity_type argument, the entity type defaults to the class name."""
    default_type_entity = NoType()
    print(f"type(default_type_entity.entity_type) == {type(default_type_entity.entity_type)}")
    print(f"default_type_entity.entity_type == {default_type_entity.entity_type}")
    assert default_type_entity.entity_type == "notype"
