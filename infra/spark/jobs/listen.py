from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaStream")
    .getOrCreate()
)

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:19092") \
    .option("subscribe", "rawjobs") \
    .load()
    
query = (
    df.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value"
    )
    .writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .option("checkpointLocation", "/tmp/checkpoint/rawjobs")
    .start()
)

query.awaitTermination()