import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import count

def process_gold():
    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    spark = SparkSession.builder.appName("BreweryGold").config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem").getOrCreate()
    
    # Lendo da Silver
    df_silver = spark.read.parquet("data/silver/breweries")
    
    # Agregação final
    df_gold = df_silver.groupBy("brewery_type", "country", "state_province") \
                       .agg(count("id").alias("brewery_count")) \
                       .orderBy("brewery_count", ascending=False)

    # Salvando na Gold
    df_gold.write.mode("overwrite").parquet("data/gold/brewery_analytics")
    
    print("Resultado da Camada Gold:")
    df_gold.show(10)
    spark.stop()

if __name__ == "__main__":
    process_gold()