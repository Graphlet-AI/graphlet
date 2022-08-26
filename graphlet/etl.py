"""Contains base classes for entities within a property graph ontology to make ETL easier."""

import typing

import pandas as pd  # type: ignore
import pandera as pa
from pandera.typing import Series


class EntitySchema(pa.SchemaModel):
    """EntitySchema - a UUID entity_id and an entity_type: node/edge."""

    entity_id: Series[str] = pa.Field(nullable=False)
    entity_type: Series[str] = pa.Field(isin=["node", "edge"], nullable=False)

    @pa.check("entity_id")
    def validate_uuid(cls, x: Series[str]) -> Series[bool]:
        """validate_uuid Validate that a string is a UUID.

        Parameters
        ----------
        x : pd.Series[str]
            A string pd.Series we hope is a UUID4

        Returns
        -------
        pd.Series[bool]
            Is a string a valid UUID?
        """

        chx: Series[bool] = x.str.contains(
            r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", regex=True, na=False
        )
        return chx


class NodeSchema(EntitySchema):
    """NodeSchema - schema for nodes."""

    pass


class EdgeSchema(EntitySchema):
    """EdgeSchema - schema for edges with src and dst UUIDs."""

    src: Series[str] = pa.Field(nullable=False)
    dst: Series[str] = pa.Field(nullable=False)


class EntityBase:
    """EntityBase - base class for ETL with Spark DataFrames with Pandera validation."""

    def __init__(self, df: pd.DataFrame, config: typing.Dict[str, typing.Any]) -> None:
        """Create ourselves with a pandas DataFrame and configuration."""
        self.df = df
        self.config = config

    def validate(self) -> pd.DataFrame:
        """validate the transformed data to verify it fits the class's schema.

        Returns
        -------
        pd.DataFrame
            Validated DataFrame or DataFrame of errors
        """
        return self.df

    def transform(self) -> pd.DataFrame:
        """transform the input DataFrame to this DataFrame's schema.

        Returns
        -------
        pd.DataFrame
            Transformed DataFrame
        """


class NodeBase:
    """NodeBase - base class for nodes."""

    pass


# class EdgeBase(pa.SchemaModel):
#     """EdgeBase - base class for edges."""

#     src: str
#     dst: str

#     @validator("src")
#     def validate_src(cls, v: str) -> str:
#         """validate_uuid Validate the source UUID.

#         Parameters
#         ----------
#         v : src entity id value
#             The value being stored in the src field

#         Returns
#         -------
#         str
#             The validated field value we return
#         """
#         if not bool(uuid_pattern.search(v)):
#             raise ValueError("Not a valid UUID")

#         return v

#     @validator("dst")
#     def validate_dst(cls, v: str) -> str:
#         """validate_uuid Validate the destination UUID.

#         Parameters
#         ----------
#         v : dst entity id value
#             The value being stored in the dst field

#         Returns
#         -------
#         str
#             The validated field value we return
#         """
#         if not bool(uuid_pattern.search(v)):
#             raise ValueError("Not a valid UUID")

#         return v
