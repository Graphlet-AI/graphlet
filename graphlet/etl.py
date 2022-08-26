"""Contains base classes for entities within a property graph ontology to make ETL easier."""

import re
import typing

import pandas as pd  # type: ignore
import pandera as pa
from pandera.typing.pandas import Series

uuid_pattern = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


def is_valid_uuid4(x: str) -> bool:
    """is_valid_uuid4 Validate that a string is a UUID4.

    Parameters
    ----------
    x : str
        A string we hope is a UUID4

    Returns
    -------
    bool
        Is a string a valid UUID?
    """

    return bool(uuid_pattern.search(x))


class EntityBase:
    """EntityBase - base class for ETL with Spark DataFrames with Pandera validation."""

    entity_id: Series[str] = pa.Field(nullable=False)
    entity_type: Series[str] = pa.Field(nullable=True)

    def __init__(self, df: pd.DataFrame, config: typing.Dict[str, typing.Any]) -> None:
        """Create ourselves with a pandas DataFrame and configuration."""
        self.df = df
        self.config = config

    @pa.check("entity_id", name="validate_uuid")
    def validate_uuid(cls, x: Series[str]) -> Series[bool]:
        """validate_uuid validate the UUID entity_id.

        Parameters
        ----------
        x : Series[str]
            the Series of strings representing UUIDs

        Returns
        -------
        Series[bool]
            the Series of pass/fail on the check
        """
        is_uuid_df: Series[bool] = x.apply(is_valid_uuid4)

        return is_uuid_df

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
