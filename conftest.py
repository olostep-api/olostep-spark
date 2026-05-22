"""Pytest configuration for olostep-spark tests."""

import os
import pytest


@pytest.fixture(scope="session")
def olostep_api_key():
    """Get Olostep API key from environment."""
    key = os.getenv("OLOSTEP_API_KEY")
    if not key:
        pytest.skip("OLOSTEP_API_KEY not set - skipping integration tests")
    return key


@pytest.fixture(scope="session")
def spark():
    """Create a Spark session for testing."""
    from pyspark.sql import SparkSession

    spark = SparkSession.builder \
        .appName("olostep-spark-tests") \
        .master("local[*]") \
        .getOrCreate()
    
    yield spark
    
    spark.stop()
