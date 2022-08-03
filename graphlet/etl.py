"""Contains base classes for entities within a property graph ontology to make ETL easier."""

from enum import Enum
from uuid import UUID, uuid4

from pydantic import Field
from pydantic_spark.base import SparkBase  # type: ignore


class EntityBase(SparkBase):
    """EntityBase - base class for ETL with Spark DataFrames.

    Includes SparkBase.spark_schema() to produce a DataFrame schema.
    """

    id_: UUID = Field(default_factory=uuid4)
    type_: Enum


class NodeBase(EntityBase):
    """NodeBase - base class for nodes."""

    pass


class EdgeBase(EntityBase):
    """EdgeBase - base class for edges."""

    src: UUID
    dst: UUID
