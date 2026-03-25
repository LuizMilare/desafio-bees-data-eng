import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit
import logging
from datetime import datetime, timezone

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_PATH = "/app/data/silver/breweries"
OUTPUT_PATH = "/app/data/gold/brewery_analytics"

def process_gold(ingestion_date: str = None) -> None:
    if ingestion_date is None:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    logger.info(f"Starting Gold transformation for ingestion_date={ingestion_date}")

    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    spark = (
        SparkSession.builder
        .appName("BreweryGold")
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    silver_partition_path = f"{INPUT_PATH}/*/ingestion_date={ingestion_date}"
    
    logger.info(f"Reading data from Silver layer partition: {silver_partition_path}")
    # Lendo da Silver
    df_silver = spark.read.parquet(INPUT_PATH)

    df_silver.createOrReplaceTempView("silver_breweries")
    
    # Agregação final
    df_gold = spark.sql(
        f"""
        SELECT 
            brewery_type,
            country,
            state_province,
            COUNT(id) as brewery_count,
            '{ingestion_date}' as ingestion_date
        FROM silver_breweries
        WHERE brewery_type IS NOT NULL
        GROUP BY brewery_type, country, state_province
        ORDER BY brewery_count DESC
        """
    ).filter(col("ingestion_date") == ingestion_date)

    record_count = df_gold.count()
    logger.info(f"Gold transformation completed with {record_count} records.")

    logger.info(f"Writing data to Gold layer at: {OUTPUT_PATH}")
    # Salvando na Gold
    df_gold.write.partitionBy("ingestion_date").mode("overwrite").parquet(OUTPUT_PATH)

    logger.info(f"Gold layer completed at: {OUTPUT_PATH}")        
    spark.stop()

if __name__ == "__main__":
    process_gold()