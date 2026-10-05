from __future__ import annotations

from datetime import datetime

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    BooleanType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


@pytest.fixture(scope="session")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    return spark


@pytest.fixture(scope="session")
def spark_session(spark):
    """Alias for spark fixture to maintain compatibility with existing tests."""
    return spark


@pytest.fixture(scope="session")
def scd2_schema():
    target_schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("employee_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("place", StringType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("valid_until", TimestampType(), True),
            StructField("active_flag", BooleanType(), False),
            StructField("delete_flag", BooleanType(), False),
            StructField("row_hash", StringType(), True),
            StructField("upsert_flag", StringType(), False),
        ]
    )
    return target_schema


@pytest.fixture(scope="session")
def primary_key():
    return "employee_id"


@pytest.fixture(scope="session")
def date_attribute():
    return "snapshot_date"


@pytest.fixture(scope="session")
def expected_data_null_end_date():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    target_schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("employee_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("place", StringType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("valid_until", TimestampType(), True),
            StructField("active_flag", BooleanType(), False),
            StructField("delete_flag", BooleanType(), False),
            StructField("row_hash", StringType(), True),
            StructField("upsert_flag", StringType(), False),
        ]
    )

    data_expected = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-01", "%Y-%m-%d"),
            "valid_until": None,
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "f747576f601c091be365c57383bb895cecaf3f696ef23e9b002a4420667e115c",
            "upsert_flag": "I",
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "amsterdam",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-02", "%Y-%m-%d"),
            "valid_until": None,
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "c2c3a3aaaa2c7fea619fd38f2f175ab1d76203016193ef5eba764a503d97bead",
            "upsert_flag": "I",
        },
    ]
    df = spark.createDataFrame(data_expected, schema=target_schema)
    return df


@pytest.fixture(scope="session")
def expected_data_open_end_date():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    target_schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("employee_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("place", StringType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("valid_until", TimestampType(), False),
            StructField("active_flag", BooleanType(), False),
            StructField("delete_flag", BooleanType(), False),
            StructField("row_hash", StringType(), True),
            StructField("upsert_flag", StringType(), False),
        ]
    )

    data_expected = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-01", "%Y-%m-%d"),
            "valid_until": datetime.strptime("9999-12-31", "%Y-%m-%d"),
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "f747576f601c091be365c57383bb895cecaf3f696ef23e9b002a4420667e115c",
            "upsert_flag": "I",
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "amsterdam",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-02", "%Y-%m-%d"),
            "valid_until": datetime.strptime("9999-12-31", "%Y-%m-%d"),
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "c2c3a3aaaa2c7fea619fd38f2f175ab1d76203016193ef5eba764a503d97bead",
            "upsert_flag": "I",
        },
    ]
    df = spark.createDataFrame(data_expected, schema=target_schema)
    return df


@pytest.fixture(scope="session")
def expected_data_catchup_one_day():
    spark = (
        SparkSession.builder.master("local[1]")
        .config("spark.port.maxRetries", "1000")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    target_schema = StructType(
        [
            StructField("country", StringType(), True),
            StructField("employee_id", LongType(), True),
            StructField("first_name", StringType(), True),
            StructField("last_name", StringType(), True),
            StructField("place", StringType(), True),
            StructField("valid_from", TimestampType(), True),
            StructField("valid_until", TimestampType(), False),
            StructField("active_flag", BooleanType(), False),
            StructField("delete_flag", BooleanType(), False),
            StructField("row_hash", StringType(), True),
            StructField("upsert_flag", StringType(), False),
        ]
    )

    data_expected = [
        {
            "employee_id": 100,
            "first_name": "ramu",
            "last_name": "kamath",
            "place": "voorburg",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-01", "%Y-%m-%d"),
            "valid_until": datetime.strptime("2022-01-03", "%Y-%m-%d"),
            "active_flag": False,
            "delete_flag": True,
            "row_hash": "f747576f601c091be365c57383bb895cecaf3f696ef23e9b002a4420667e115c",
            "upsert_flag": "U",
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "amsterdam",
            "country": "netherlands",
            "valid_from": datetime.strptime("2022-01-02", "%Y-%m-%d"),
            "valid_until": datetime.strptime("2022-01-03", "%Y-%m-%d"),
            "active_flag": False,
            "delete_flag": False,
            "row_hash": "c2c3a3aaaa2c7fea619fd38f2f175ab1d76203016193ef5eba764a503d97bead",
            "upsert_flag": "U",
        },
        {
            "employee_id": 200,
            "first_name": "deena",
            "last_name": "mehroof",
            "place": "mangalore",
            "country": "india",
            "valid_from": datetime.strptime("2022-01-03", "%Y-%m-%d"),
            "valid_until": datetime.strptime("9999-12-31", "%Y-%m-%d"),
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "bbbbac539154f0fc817ed6365c9fd794a50424156c8a10c3ed0c70cfc427d1db",
            "upsert_flag": "I",
        },
        {
            "employee_id": 300,
            "first_name": "akansha",
            "last_name": "sahoo",
            "place": "bangalore",
            "country": "india",
            "valid_from": datetime.strptime("2022-01-03", "%Y-%m-%d"),
            "valid_until": datetime.strptime("9999-12-31", "%Y-%m-%d"),
            "active_flag": True,
            "delete_flag": False,
            "row_hash": "e6fd0c58d77528622c3cd33dcd567d6046256bd87d55d5de675015b11abed6ee",
            "upsert_flag": "I",
        },
    ]
    df = spark.createDataFrame(data_expected, schema=target_schema)
    return df
