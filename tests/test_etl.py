"""Implements unit tests of Graphlet's spark module."""

import random
import typing

# from typing import TypeVar
from uuid import uuid4

import pandas as pd  # type: ignore
import pandera as pa
import pyspark.sql.functions as F
import pyspark.sql.types as T
import pytest
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import PandasUDFType

from graphlet.etl import EdgeSchema, EntitySchema, NodeSchema


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


@pytest.fixture
def get_good_entity_df() -> pd.DataFrame:
    """Get a DataFrame fit for an EntitySchema's validation."""
    return pd.DataFrame(
        [
            {
                "entity_id": str(uuid4()),
                "entity_type": "node",
            }
            for x in range(0, 5)
        ]
    )


def test_good_entity_schema(get_good_entity_df) -> None:
    """Test the entity schema using a pd.DataFrame with all good records."""

    @pa.check_types(lazy=True)
    def transform(df: pa.typing.DataFrame[EntitySchema]) -> pa.typing.DataFrame[EntitySchema]:
        return df

    transform(get_good_entity_df)


def bad_entity_df(bad_id: bool, null_id: bool, bad_type: bool, null_type: bool) -> pd.DataFrame:
    """get_test_name_and_bad_entity_df Get a DataFrame unit for an EntitySchema's validation.

    Call me via:

    # Get a DataFrame with 5 good records and one bad entity_id
    @pytest.mark.parametrize("bad_id, null_id, bad_type, null_type", [True, False, False, False])
    def test_dataframe(get_bad_entity_df) -> None:
        ...

    Parameters
    ----------
    test_name: str
        The name of the test
    bad_id : bool
        Add a record with a bad entity_id, by default False
    null_id : bool
        Add a record with a null entity_id, by default False
    bad_type : bool
        Add a record with a bad entity_type, by default False
    null_type : bool
        Add a record with a null entity_type, by default False

    Returns
    -------
    pd.DataFrame
        A test DataFrame with good and whichever bad records we ask for
    """

    # Start out with some good records...
    records: typing.List[typing.Dict[str, typing.Union[str, None]]] = [
        {
            "entity_id": str(uuid4()),
            "entity_type": "node",
        }
        for x in range(0, 4)
    ]

    # And add whatever bad records we ask for :)
    if bad_id:
        records.append({"entity_id": "not-a-uuid", "entity_type": "node"})

    if null_id:
        records.append({"entity_id": None, "entity_type": "node"})

    if bad_type:
        records.append({"entity_id": str(uuid4()), "entity_type": "foobar"})

    if null_type:
        records.append({"entity_id": str(uuid4()), "entity_type": None})

    return pd.DataFrame(records)


@pytest.mark.parametrize(
    "test_name, bad_id, null_id, bad_type, null_type",
    [
        ("bad_id", True, False, False, False),
        ("null_id", False, True, False, False),
        ("bad_type", False, False, True, False),
        ("null_type", False, False, False, True),
    ],
)
def test_bad_entity_schema(test_name, bad_id, null_id, bad_type, null_type) -> None:
    """Test the entity schema with four different versions of bad data."""

    @pa.check_types(lazy=True)
    def transform(df: pa.typing.DataFrame[EntitySchema]) -> pa.typing.DataFrame[EntitySchema]:
        return df

    # Use the arguments to get a pd.DataFrame with the right kind of errors
    error_df = bad_entity_df(bad_id, null_id, bad_type, null_type)

    try:
        transform(error_df)
    except pa.errors.SchemaErrors as e:
        error_df = e.failure_cases

        error_case = error_df.iloc[0]["failure_case"]

        # Did it detect a non-UUID entity_id?
        if test_name == "bad_id":
            assert error_case == "not-a-uuid"

        # Did it detect a null entity_id?
        if test_name == "null_id":
            assert error_case is None

        # Is entity_type outside of node/edge?
        if test_name == "bad_type":
            assert error_case == "foobar"

        # Is entity_type null?
        if test_name == "null_type":
            assert error_case is None


@pytest.fixture
def get_good_edge_df() -> pd.DataFrame:
    """get_good_edge_df Generate a pd.DataFrame full of valid edges.

    Returns
    -------
    pd.DataFrame
        A DataFrame of valid edges
    """
    records: pd.DataFrame = pd.DataFrame(
        [
            {
                "entity_id": str(uuid4()),
                "entity_type": "edge",
                "src": str(uuid4()),
                "dst": str(uuid4()),
            }
            for x in range(0, 4)
        ]
    )
    return records


def test_transformed_edge_schema(get_good_edge_df) -> None:
    """Test the entity schema using a pd.DataFrame with all good records."""

    class WeightedEdgeSchema(EdgeSchema):
        weight: pa.typing.Series[float] = pa.Field(gt=0)

    @pa.check_types(lazy=True)
    def transform(df: pa.typing.DataFrame[EdgeSchema]) -> pa.typing.DataFrame[WeightedEdgeSchema]:
        df["weight"] = df["entity_id"].apply(lambda x: random.uniform(0, 1))
        return df

    transform(get_good_edge_df)


@pytest.fixture
def get_good_spark_df(spark_session_context):
    """Get a DataFrame fit for an EntitySchema's validation."""

    spark: SparkSession = spark_session_context[0]

    return spark.createDataFrame(
        pd.DataFrame(
            [
                {
                    "entity_id": str(uuid4()),
                    "entity_type": "node",
                }
                for x in range(0, 5)
            ]
        )
    )


def test_pandera_pyspark(get_good_spark_df):
    """test_pandera_pyspark test Pandera's PySpark DataFrame support.

    Parameters
    ----------
    get_good_spark_df : _type_
        _description_

    Returns
    -------
    _type_
        _description_
    """

    class PersonSchema(NodeSchema):

        name: pa.typing.Series[str] = pa.Field(
            str_length=3,
        )

    from pandera.typing.pyspark import DataFrame

    @pa.check_types(lazy=True)
    def transform(df: DataFrame[NodeSchema]) -> DataFrame[PersonSchema]:

        df["testola"] = 7
        return df

    transform(get_good_spark_df)
