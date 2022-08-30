"""Contains base classes for entities within a property graph ontology to make ETL easier."""

import typing

# import pandas as pd  # type: ignore
import pandera as pa
from pandera.typing import DataFrame, Index, Series


class EntitySchema(pa.SchemaModel):
    """EntitySchema - base class for nodes and edges.

    I contain three simple things:

    * An index
    * A UUID entity_id
    * A string entity_type with valida values of node or edge.
    """

    index: Index[int]
    entity_id: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )
    entity_type: Series[str] = pa.Field(isin=["node", "edge"], nullable=False)


class NodeSchema(EntitySchema):
    """NodeSchema - schema for nodes."""

    entity_type: Series[str] = pa.Field(isin=["node"], nullable=False)


class EdgeSchema(EntitySchema):
    """EdgeSchema - schema for edges with src and dst UUIDs."""

    entity_type: Series[str] = pa.Field(isin=["edge"], nullable=False)
    src: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )
    dst: Series[str] = pa.Field(
        nullable=False,
        str_matches=r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    )


class EntityBase:
    """EntityBase - static base class for ETL with Spark DataFrames with Pandera validation."""

    schema: typing.Type[EntitySchema] = EntitySchema

    @pa.check_types(lazy=True)
    def ingest(cls, df: DataFrame[EntitySchema]) -> DataFrame[EntitySchema]:
        """ingest stub method to ingest raw data to build an entity.

        This shouldn't be used, it is a stub.

        Returns
        -------
        pa.typing.DataFrame
            Validated DataFrame or DataFrame of errors - or is it?
        """
        return df


class NodeBase(EntityBase):
    """NodeBase - base class for nodes."""

    schema: typing.Type[NodeSchema] = NodeSchema

    @pa.check_types(lazy=True)
    def ingest(cls, df: DataFrame[NodeSchema]) -> DataFrame[NodeSchema]:
        """ingest stub method to ingest raw data to build an entity.

        This shouldn't be used, it is a stub.

        Returns
        -------
        pa.typing.DataFrame
            Validated DataFrame or DataFrame of errors - or is it?
        """
        return df


class EdgeBase(EntityBase):
    """EdgeBase - base class for edges."""

    schema: typing.Type[EdgeSchema] = EdgeSchema

    @pa.check_types(lazy=True)
    def ingest(cls, df: DataFrame[EdgeSchema]) -> DataFrame[EdgeSchema]:
        """ingest stub method to ingest raw data to build an entity.

        This shouldn't be used, it is a stub.

        Returns
        -------
        pa.typing.DataFrame
            Validated DataFrame or DataFrame of errors - or is it?
        """
        return df
