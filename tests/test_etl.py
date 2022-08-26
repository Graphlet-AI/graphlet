"""Implements unit tests of Graphlet's spark module."""

import typing

import pandas as pd  # type: ignore
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType

from graphlet.etl import EntityBase, NodeBase  # EdgeBase, EntityBase, NodeBase


@pytest.fixture
def spark_session_context(app_name="PyTest fixture SparkSession") -> typing.Tuple[SparkSession, SparkContext]:
    """spark_session_context generate a SparkSession its SparkContext for unit tests.

    Parameters
    ----------
    app_name : str, optional
        Spark application name, by default "PyTest fixture SparkSession"

    Returns
    -------
    typing.Tuple[SparkSession, SparkContext]
        A SparkSession and SparkContext in a local environment
    """

    spark = SparkSession.builder.appName(app_name).getOrCreate()
    sc = spark.sparkContext
    return spark, sc


def test_spark_session_fixture(spark_session_context: typing.Tuple[SparkSession, SparkContext]) -> None:
    """test_spark_session_fixture Make sure the SparkSession is created."""

    spark, sc = spark_session_context

    data = [("a", "b"), ("c", "d")]
    df = spark.createDataFrame(data, ["x", "y"])
    assert df.count() == 2
    assert df.collect() == [("a", "b"), ("c", "d")]


# def test_entity_id() -> None:
#     """test_entity_base_id Verify that the EntityBase auto generated entity_id is a valid UUID4."""

#     # Verify that the UUID4 works
#     entity = TestEntity()
#     assert re.search(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", entity.entity_id)


# def test_entity_base_enum() -> None:
#     """test_entity_base Verify that the EntityBase types are working."""

#     # Verify that the type is as expected
#     entity1 = TestEntity()
#     assert entity1.entity_id is not None
#     assert entity1.entity_type == "test_entity"

#     # Verify that the type is as expected
#     try:
#         TestEntity(entity_type=None)
#     except ValueError:
#         pytest.fail("Set null entity type")


# class Movie(NodeBase):
#     """Movie genres."""

#     entity_type = "movie"
#     title: str
#     genre: typing.Optional[str]


# def test_node() -> None:
#     """test_node Test the EdgeBase class."""

#     comedy_node = Movie(title="Coming to America", genre="comedy")
#     assert comedy_node.genre == "comedy"


# class Director(NodeBase):
#     """A person in hollywood."""

#     entity_type = "director"
#     name: str
#     nationality: typing.Optional[str]


# class Directed(EdgeBase):
#     """A director directed a movie."""

#     entity_type = "directed"


# def test_nodes_edge() -> None:
#     """test_edge Test the Relationship class."""

#     comedy_node = Movie(title="Trauma", genre="horror")
#     director_node = Director(name="Dario Argento")
#     director_edge = Directed(src=director_node.entity_id, dst=comedy_node.entity_id, entity_type="director")

#     # Test ze nodes
#     assert comedy_node.genre == "horror"
#     assert director_node.name == "Dario Argento"

#     # Test ze edge
#     assert director_edge.src == director_node.entity_id
#     assert director_edge.dst == comedy_node.entity_id


# class NoType(EntityBase):
#     """An entity without a specified type."""

#     pass


def standard_unrated(x):
    """standard_unrated Standardize different forms of unrated films.

    Parameters
    ----------
    x : str
        movie rating

    Returns
    -------
    str
        The standard rating.
    """
    rating: str = "Unknown"
    if ("not" in x.lower()) or ("un" in x.lower()):
        rating = "Unrated"

    if x.upper() in ["G", "PG", "PG-13", "R", "X", "XX", "XXX"]:
        rating = x.upper()

    return rating


@F.udf(T.StringType())
def stanard_unrated_udf(x):
    """stanard_unrated_udf UDF that cleans up movie ratings.

    Parameters
    ----------
    x : str
        The rating of the movie to be cleaned
    """

    return standard_unrated(x)


def text_runtime_to_minutes(x: str) -> int:
    """text_runtime_to_minutes Turn a text runtime to minutes.

    Parameters
    ----------
    x : str
        Raw text movie runtime field: ex. "1h 34m"

    Returns
    -------
    int
        minutes of runtime
    """
    hour_min = x.split(" ")
    hours = int(hour_min[0][:-1])
    mins = int(hour_min[1][:-1])

    return (60 * hours) + mins


@F.udf(T.LongType())
def text_runtime_to_minutes_old_udf(x: str) -> int:
    """Normal PySpark UDF to convert text runtime to integer minutes."""
    return text_runtime_to_minutes(x)


def test_traditional_spark_etl(spark_session_context: typing.Tuple[SparkSession, SparkContext]) -> None:
    """Test the classes with Spark UDFs."""

    spark, sc = spark_session_context

    # A genre of movies
    comedies = spark.read.option("header", "true").csv("tests/data/comedy.csv")
    comedies.show()

    # Another genre of movies
    horror = spark.read.option("header", "true").csv("tests/data/horror.csv")
    horror.show()

    # Transform comedies into generic movies
    comedy_movies = comedies.select(
        F.lit("movie").alias("entity_type"),
        F.lit("comady").alias("genre"),
        "title",
        "year",
        "length",
        "gross",
        F.lit(None).alias("rating"),
    )
    comedy_movies.show()

    # Transform horror films into generic movies
    horror_movies = horror.select(
        F.lit("movie").alias("entity_type"),
        F.lit("horror").alias("genre"),
        F.col("Title").alias("title"),
        F.col("Year").alias("year"),
        text_runtime_to_minutes_old_udf("Length").alias("length"),
        F.lit(None).alias("gross"),
        stanard_unrated_udf("Rating").alias("rating"),
    )
    horror_movies.show()


def test_pandas_spark_etl(spark_session_context: typing.Tuple[SparkSession, SparkContext]) -> None:
    """Test the classes with Spark UDFs."""

    spark, sc = spark_session_context

    @F.pandas_udf(T.IntegerType(), PandasUDFType.SCALAR)
    def text_runtime_to_minutes_pandas_udf(x: pd.Series) -> pd.Series:
        """text_runtime_to_minutes_pandas_udf pandas_udf that runs text_runtime_to_minutes.

        Parameters
        ----------
        x : pd.Series[str]
            A series of waw text movie runtime field: ex. "1h 34m"

        Returns
        -------
        pd.Series[int]
            A series of minutes of runtime
        """
        return x.apply(text_runtime_to_minutes).astype("int")

    @F.pandas_udf("string", PandasUDFType.SCALAR)
    def stanard_unrated_pandas_udf(x: pd.Series[str]) -> pd.Series[str]:
        """stanard_unrated_udf UDF that cleans up movie ratings.

        Parameters
        ----------
        x : str
            The rating of the movie to be cleaned
        """

        return x.apply(standard_unrated).astype("str")

    # Another genre of movies
    horror = spark.read.option("header", "true").csv("tests/data/horror.csv")
    horror.show()

    # Transform horror films into generic movies
    horror_movies = horror.select(
        F.lit("movie").alias("entity_type"),
        F.lit("horror").alias("genre"),
        F.col("Title").alias("title"),
        F.col("Year").alias("year"),
        text_runtime_to_minutes_pandas_udf("Length").alias("length"),
        F.lit(None).alias("gross"),
        stanard_unrated_pandas_udf("Rating").alias("rating"),
    )
    horror_movies.show()


# def test_base_classes() -> None:
#     """Test the graplet.etl base classes for nodes and edges."""


# def test_graphlet_etl(spark_session_context) -> None:
#     """Test the classes with Spark UDFs."""

#     spark, sc = spark_session_context

#     @F.pandas_udf("long")
#     def text_runtime_to_minutes_pandas_udf(x: pd.Series) -> pd.Series:
#         """text_runtime_to_minutes_pandas_udf PySpark pandas_udf to run text_runtime_to_minutes.

#         Parameters
#         ----------
#         x: pd.Series
#             Column to run text_runtime_to_minutes on

#         Returns
#         -------
#         pd.Series
#             a Column of integers
#         """
#         return x.apply(text_runtime_to_minutes)

#     # Movie awards
#     awards = spark.read.option("header", "true").csv("tests/data/awards.csv")
#     awards.show()

#     # A genre of movies
#     comedies = spark.read.option("header", "true").csv("tests/data/comedy.csv")
#     comedies.show()

#     # Another genre of movies
#     horror = spark.read.option("header", "true").csv("tests/data/horror.csv")
#     horror.show()

#     class Movie(NodeBase):
#         """A film node in hollywood."""

#         entity_type: str = "movie"
#         genre: str
#         title: str
#         year: str
#         length: int = 0
#         gross: int = 0
#         rating: str

#         @validator("length", pre=True)
#         def convert_hours_minutes_to_int_minutes(cls, x):
#             if x and isinstance(x, str):
#                 x = text_runtime_to_minutes(x)
#             return x

#     class Person(NodeBase):
#         """A class about people in the film industry."""

#         entity_type = "person"
#         name: str
#         role: str

#     class Directed(EdgeBase):
#         """A director directed a movie."""

#         entity_type = "directed"

#     class ActedIn(EdgeBase):
#         """An actor acted in a movie."""

#         entity_type = "acted_in"

#     def horror_to_movie(dfs: typing.Iterable[pd.DataFrame]) -> typing.Iterable[pd.DataFrame]:
#         """horror_to_movie Iterates entire pandas DataFrames, transforming from original schema to the pydantic schema.

#         Parameters
#         ----------
#         x : typing.Iterable[pd.DataFrame]
#             Accepts an iterable of pd.DataFrames

#         Returns
#         -------
#         typing.Iterable[pd.DataFrame]

#         Yields
#         ------
#         Iterator[typing.Iterable[pd.DataFrame]]
#             pd.DataFrames of the new schema
#         """

#         def map_to_movie(x):
#             """Map the DataFrame rows to a Movie, validate them, then back out to a dict."""
#             print(x, type(x))
#             return Movie(
#                 genre="horror",
#                 title=x["Title"],
#                 year=x["Year"],
#                 length=x["Length"],
#                 rating=x["Rating"],
#             ).dict()

#         for df in dfs:
#             # yield pd.concat(pd.DataFrame(x) for x in df.map(map_to_movie))
#             yield df.apply(map_to_movie, axis=1)

#     movie_schema = T.StructType.fromJson(Movie.spark_schema())

#     horror_movies = horror.mapInPandas(horror_to_movie, schema=movie_schema)
#     horror_movies.show()
