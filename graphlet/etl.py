"""Contains base classes for entities within a property graph ontology to make ETL easier."""

import re
from enum import Enum
from uuid import uuid4

from pydantic import Field, validator
from pydantic_spark.base import SparkBase  # type: ignore

uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class EntityBase(SparkBase):
    """EntityBase - base class for ETL with Spark DataFrames.

    Includes SparkBase.spark_schema() to produce a DataFrame schema.
    """

    class Config:
        """Configure the class to use Enum values."""

        use_enum_values = True

    entity_id: str = Field(default_factory=lambda: str(uuid4()))
    entity_type: Enum

    @validator("entity_id")
    def validate_uuid(cls, v: str) -> str:
        """validate_uuid Validate the UUID.

        Parameters
        ----------
        v : entity_id value
            The value being stored in the entity_id field

        Returns
        -------
        str
            The validated field value we return
        """
        if not bool(uuid_pattern.search(v)):
            raise ValueError("Not a valid UUID")

        return v


class NodeBase(EntityBase):
    """NodeBase - base class for nodes."""

    pass


class EdgeBase(EntityBase):
    """EdgeBase - base class for edges."""

    src: str
    dst: str
