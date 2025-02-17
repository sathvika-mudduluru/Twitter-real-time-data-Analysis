

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
 
spark = SparkSession.builder.appName("TwittterAnalysis").getOrCreate()
 
spark.sparkContext.setLogLevel('ERROR')
 
kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "localhost:9092").option("subscribe","new").load()
 
kafkaDF_String = kafkaDF.selectExpr("CAST(value AS STRING)")
 
tweetsTable = kafkaDF_String.alias("tweets")
 
groupingHashtags = tweetsTable.groupBy("value").count().orderBy(desc("count"))
 
query = groupingHashtags.writeStream.outputMode("complete").format("console").option("truncate", "false").start()
 
query.awaitTermination()

