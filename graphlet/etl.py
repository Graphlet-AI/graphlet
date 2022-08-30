"""Contains base classes for entities within a property graph ontology to make ETL easier."""

# import typing

# import pandas as pd  # type: ignore
import pandera as pa
from pandera.typing import Index, Series


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


# class EntityBase:
#     """EntityBase - static base class for ETL with Spark DataFrames with Pandera validation."""

#     schema: typing.Type[EntitySchema] = EntitySchema

#     @classmethod
#     def ingest(cls, df: pd.DataFrame) -> pd.DataFrame:
#         """ingest raw data to build an entity.

#         Returns
#         -------
#         pd.DataFrame
#             Validated DataFrame or DataFrame of errors - or is it?
#         """
#         return df


# class NodeBase(EntityBase):
#     """NodeBase - base class for nodes."""

#     schema: typing.Type[NodeSchema] = NodeSchema


# class EdgeBase(EntityBase):
#     """EdgeBase - base class for edges."""

#     schema: typing.Type[EdgeSchema] = EdgeSchema
