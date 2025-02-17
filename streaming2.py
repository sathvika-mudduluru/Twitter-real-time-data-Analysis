from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, to_json, struct, count
from pyspark.sql.types import *

spark = SparkSession .builder.appName("twitter").getOrCreate()

raw_df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe", "twitter").option("startingOffsets", "latest").load().selectExpr("CAST(value AS STRING)")
