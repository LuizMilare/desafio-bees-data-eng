import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, lit

import logging
from datetime import datetime, timezone

from scripts.spark_utils import get_spark

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
    spark = get_spark("BreweryGold")

    spark.sparkContext.setLogLevel("ERROR")
    
    logger.info(f"Reading data from Silver layer partition: {INPUT_PATH}/ingestion_date={ingestion_date}")
    # Lendo da Silver
    df_silver = spark.read.parquet(INPUT_PATH).filter(col("ingestion_date") == ingestion_date)

    df_silver.createOrReplaceTempView("silver_breweries")
    
    # Agregação final
    df_gold = spark.sql(
        f"""
        SELECT 
            brewery_type,
            country,
            state_province,
            COUNT(id) as brewery_count
        FROM silver_breweries
        WHERE brewery_type IS NOT NULL
        GROUP BY brewery_type, country, state_province
        ORDER BY brewery_count DESC
        """
    ).withColumn("ingestion_date", lit(ingestion_date))

    record_count = df_gold.count()
    logger.info(f"Gold transformation completed with {record_count} records.")

    logger.info(f"Writing data to Gold layer at: {OUTPUT_PATH}")
    # Salvando na Gold
    df_gold.write.partitionBy("ingestion_date").mode("overwrite").parquet(OUTPUT_PATH)

    logger.info(f"Gold layer completed at: {OUTPUT_PATH}")        
    spark.stop()

if __name__ == "__main__":
    process_gold()