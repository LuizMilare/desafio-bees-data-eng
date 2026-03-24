from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

def process_silver():
    #Inicia a sessão Spark. Com configurações adicionais para os primeiros testes locais em Docker
    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    spark = SparkSession.builder.appName("BrewerySilver").config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem").config("spark.driver.host", "127.0.0.1").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    print("Lendo dados da camada Bronze...")
    # 1. Ler o JSON da bronze
    df = spark.read.option("multiline", "true").json("/app/data/bronze/breweries_raw.json")

    # 2. Transformações simples (data cleaning)
    # Garantindo que 'country' não seja nulo para o particionamento
    df_cleaned = df.withColumn("country", when(col("country").isNull(), "unknown").otherwise(col("country")))

    # 3. Escrita na Silver, particionados por país - Partição pode ser alterada futuramente para melhora de desempenho
    output_path = "/app/data/silver/breweries"

    print("Escrevendo dados na camada Silver em Parquet particionando por 'country'...")

    df_cleaned.write.partitionBy("country").mode("overwrite").parquet(output_path)

    print(f"Camada Silver concluída em: {output_path}")

    spark.stop()

if __name__ == "__main__":
    process_silver()