from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, lit
from pyspark.sql.types import StructType, StructField, StringType

import os
import logging
from datetime import datetime, timezone

from scripts.spark_utils import get_spark

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

INPUT_PATH = "/app/data/bronze/breweries_raw.json"
OUTPUT_PATH = "/app/data/silver/breweries"

def process_silver(ingestion_date: str = None) -> None:

    if ingestion_date is None:
        ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    logger.info(f"Starting transformation for ingestion_date={ingestion_date}")
    
    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    spark = get_spark("BrewerySilver")

    spark.sparkContext.setLogLevel("ERROR")

    # 1. Define Schema
    schema = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("brewery_type", StringType(), True),
    StructField("address_1", StringType(), True),
    StructField("address_2", StringType(), True),
    StructField("address_3", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state_province", StringType(), True),
    StructField("postal_code", StringType(), True),
    StructField("country", StringType(), True),
    StructField("longitude", StringType(), True),
    StructField("latitude", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("website_url", StringType(), True),
    StructField("state", StringType(), True),
    StructField("street", StringType(), True),
    ])

    logger.info("Reading data from bronze layer..")
    
    # 2. Reading the JSON from the Bronze layer
    df = (
        spark.read
        .schema(schema)
        .option("multiline", "true")
        .json("/app/data/bronze/breweries_raw.json")
    )

    # 3. Deduplicating
    df_deduped = df.dropDuplicates(["id"])

    # 4. Ensuring 'country' is not null for partitioning
    df_cleaned = df_deduped.withColumn(
        "country",
        when(col("country").isNull(), "unknown").otherwise(col("country")))

    # 5. Writing to Silver layer, partitioned by country - Partitioning can be changed in the future for performance improvements
    output_path = "/app/data/silver/breweries"

    # 6. Add ingestion_date column for partitioning and future auditing
    df_final = df_cleaned.withColumn("ingestion_date", lit(ingestion_date))

    record_count = df_final.count()
    logger.info(f"Transformation completed with {record_count} records.")

    df_final.write.partitionBy("country", "ingestion_date").mode("overwrite").parquet(OUTPUT_PATH)

    logger.info(f"Silver layer completed at: {output_path}")

    spark.stop()

if __name__ == "__main__":
    process_silver()