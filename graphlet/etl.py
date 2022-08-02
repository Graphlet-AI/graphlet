"""Contains a base class for entities within an ontology to make ETL easier."""

from uuid import UUID, uuid4

from pydantic import Field
from pydantic_spark.base import SparkBase  # type: ignore


class EntityBase(SparkBase):
    """EntityBase - base class for ETL with Spark DataFrames.

    Includes SparkBase.spark_schema() to produce a DataFrame schema.
    """

    id_: UUID = Field(default_factory=uuid4)
