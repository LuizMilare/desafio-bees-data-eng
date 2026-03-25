import os
import json
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()

# Bronze layer tests
def test_bronze_file_exists():
    # Checks for the existence of the raw file
    assert os.path.exists("/app/data/bronze/breweries_raw.json")

def test_json_is_not_empty():
    # Verifies that there is data in the raw JSON
    import json
    with open("/app/data/bronze/breweries_raw.json", "r") as f:
        data = json.load(f)
        assert len(data) > 0

# Silver layer tests
def test_silver_parquet_exists():
    # Checks for the existence of the folder with the partitions
    assert os.path.isdir("/app/data/silver/breweries")

def test_silver_duplicates(spark):
    # Checks if the IDs are unique
    df = spark.read.parquet("/app/data/silver/breweries")
    ids = df.select("id").distinct().count()
    total = df.count()
    assert ids == total

def test_silver_schema(spark):
    # Verification of the existence of the main columns according to the API documentation
    df = spark.read.parquet("/app/data/silver/breweries")
    expected_columns = ["id", "name", "brewery_type", "state", "city"]
    for col in expected_columns:
        assert col in df.columns

# Gold layer tests
def test_gold_aggregation(spark):
    # Verifies the aggregation of the gold layer
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    # If the aggregation is changed, it is necessary to adapt the test
    assert "brewery_count" in df.columns or "count" in df.columns