from pyspark.sql import SparkSession
import os

def get_spark(app_name: str) -> SparkSession:
    os.environ['HADOOP_HOME'] = os.path.abspath(os.getcwd())
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.RawLocalFileSystem")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )