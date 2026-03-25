from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import *
import os

def process_silver():
    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    spark = SparkSession.builder.appName("BrewerySilver").config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem").config("spark.driver.host", "127.0.0.1").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Reading data from the Bronze layer...")
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
    
    # 2. Reading the JSON from the Bronze layer
    df = spark.read.schema(schema).option("multiline", "true").json("/app/data/bronze/breweries_raw.json")

    # 3. Deduplicating
    df_deduped = df.dropDuplicates(["id"])

    # 4. Ensuring 'country' is not null for partitioning
    df_cleaned = df_deduped.withColumn("country", when(col("country").isNull(), "unknown").otherwise(col("country")))

    # 5. Writing to Silver layer, partitioned by country - Partitioning can be changed in the future for performance improvements
    output_path = "/app/data/silver/breweries"

    print("Writing data to the Silver layer in Parquet format, partitioned by 'country'...")

    df_cleaned.write.partitionBy("country").mode("overwrite").parquet(output_path)

    print(f"Silver layer completed at: {output_path}")

    spark.stop()

if __name__ == "__main__":
    process_silver()