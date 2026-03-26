import os
import json
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import count, col

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()

# Bronze layer tests
def test_bronze_file_exists():
    # Checks for the existence of the raw file
    ingestion_date = os.environ.get("INGESTION_DATE")
    assert os.path.exists(f"/app/data/bronze/ingestion_date={ingestion_date}/breweries_raw.json")

def test_json_is_not_empty():
    # Verifies that there is data in the raw JSON
    import json
    ingestion_date = os.environ.get("INGESTION_DATE")
    with open(f"/app/data/bronze/ingestion_date={ingestion_date}/breweries_raw.json", "r") as f:
        data = json.load(f)
        assert len(data) > 0

# Silver layer tests
def test_silver_parquet_exists():
    # Checks for the existence of parquet files in each partition
    base = "/app/data/silver/breweries"
    assert os.path.isdir(base)
    parquet_files = [
        f for root, _, files in os.walk(base)
        for f in files if f.endswith(".parquet")
    ]
    assert len(parquet_files) > 0


def test_silver_duplicates(spark):
    # Checks if the IDs are unique
    df = spark.read.parquet("/app/data/silver/breweries")
    duplicates = df.groupBy("id", "ingestion_date").agg(count("id").alias("ocurrencies")).filter(col("ocurrencies") > 1)
    assert duplicates.count() == 0

def test_silver_schema(spark):
    # Verification of the existence of the main columns according to the API documentation
    df = spark.read.parquet("/app/data/silver/breweries")
    expected_columns = ["id", "name", "brewery_type", "state", "city"]
    for col_name in expected_columns:
        assert col_name in df.columns

# Gold layer tests
def test_gold_aggregation(spark):
    # Verifies the aggregation of the gold layer
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    # If the aggregation is changed, it is necessary to adapt the test
    assert "brewery_count" in df.columns or "count" in df.columns

def test_gold_aggregations_positive(spark):
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    assert df.filter(col("brewery_count") <= 0).count() == 0

def test_gold_no_null_brewery_type(spark):
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    assert df.filter(col("brewery_type").isNull()).count() == 0

def test_gold_has_data_for_today(spark):
    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    count = df.filter(col("ingestion_date") == today).count()
    assert count > 0