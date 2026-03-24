import os
import json
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[*]").appName("Testing").getOrCreate()

# Testes da camada bronze
def test_bronze_file_exists():
    # Verifica existência do arquivo raw
    assert os.path.exists("/app/data/bronze/breweries_raw.json")

def test_json_is_not_empty():
    # Verifica se há dados no JSON raw
    import json
    with open("/app/data/bronze/breweries_raw.json", "r") as f:
        data = json.load(f)
        assert len(data) > 0

# Testes da camada silver
def test_silver_parquet_exists():
    # Verifica a existência da pasta com as partições
    assert os.path.isdir("/app/data/silver/breweries")

def test_silver_schema(spark):
    # Verificação da existência das principais colunas de acordo com documentação da API
    df = spark.read.parquet("/app/data/silver/breweries")
    expected_columns = ["id", "name", "brewery_type", "state", "city"]
    for col in expected_columns:
        assert col in df.columns

# --- TESTES DA CAMADA GOLD ---
def test_gold_aggregation(spark):
    # Verifica a agregação da camada gold
    df = spark.read.parquet("/app/data/gold/brewery_analytics")
    # Caso a agregação seja alterada, é necessário adaptar o teste
    assert "brewery_count" in df.columns or "count" in df.columns