from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    ArrayType,
    TimestampType
)
from pyspark.sql.functions import (
    from_json,
    col,
    trim,
    lower
)

spark = (
    SparkSession.builder
    .appName("KafkaStream")
    .getOrCreate()
)

spark.sql("""
CREATE TABLE IF NOT EXISTS polaris.bronze.jobs (
    company STRING,
    job_name STRING,
    url STRING,
    location STRING,
    details STRING,
    source STRING,
    keyword STRING,
    requirements ARRAY<STRING>,
    skills ARRAY<STRING>,
    benefits ARRAY<STRING>,
    salary STRING,
    publisher_name STRING,
    applicant INTEGER,
    quota INTEGER,
    publisher_last_online TIMESTAMP,
    listing_date TIMESTAMP,
    scraped_at TIMESTAMP
)
USING iceberg
""")

schema = StructType([
    StructField('company', StringType()),
    StructField('job_name', StringType()),
    StructField('url', StringType()),
    StructField('location', StringType()),
    StructField('details', StringType()),
    StructField('source', StringType()),
    StructField('keyword', StringType()),
    StructField('applicant', IntegerType()),
    StructField('quota', IntegerType()),
    StructField('requirements', ArrayType(StringType())),
    StructField('skills', ArrayType(StringType())),
    StructField('benefits', ArrayType(StringType())),
    StructField('publisher_name', StringType()),
    StructField('publisher_last_online', TimestampType()),
    StructField('listing_date', TimestampType()),
    StructField('scraped_at', TimestampType()),
])

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:19092") \
    .option("subscribe", "rawjobs") \
    .load()
    
jobs = (
        df
        .selectExpr("CAST(value AS STRING) AS json")
        .select(
            from_json(col="json", schema=schema).alias("data")
        )
        .select("data.*")
    )

cleaned_jobs = (
    jobs
    .withColumn("company", trim(lower(col("company"))))
    .withColumn("keyword", trim(lower(col("keyword"))))
    .withColumn("job_name", trim(lower(col("job_name"))))
)

final_job = (
    cleaned_jobs.select(
        "company",
        "job_name",
        "url",
        "location",
        "details",
        "source",
        "keyword",
        "requirements",
        "skills",
        "benefits",
        "salary",
        "publisher_name",
        "applicant",
        "quota",
        "publisher_last_online",
        "listing_date",
        "scraped_at"
    )
)
        
query = (
    final_job
    .writeStream
    .format("iceberg")
    .outputMode("append")
    .option("truncate", "false")
    .toTable("polaris.bronze.jobs")
    .option("checkpointLocation", "/tmp/checkpoint/rawjobs")
    .start()
)

query.awaitTermination()