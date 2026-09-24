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
    lower,
    current_timestamp
)

spark = (
    SparkSession.builder
    .appName("JobsKafkaIngestion")
    .config("spark.sql.adaptive.enabled", "false")
    .config("spark.cores.max", "1")
    .getOrCreate()
)

spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.bronze");

spark.sql("""
CREATE TABLE IF NOT EXISTS warehouse.bronze.jobs (
    company STRING,
    job_name STRING,
    url STRING,
    location STRING,
    city STRING,
    province STRING,
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
    topic STRING,
    kafka_key STRING,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP,
    publisher_last_online TIMESTAMP,
    listing_date TIMESTAMP,
    scraped_at TIMESTAMP,
    ingested_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (truncate(1, company))
""")

schema = StructType([
    StructField('company', StringType()),
    StructField('job_name', StringType()),
    StructField('url', StringType()),
    StructField('location', StringType()),
    StructField('city', StringType()),
    StructField('province', StringType()),
    StructField('details', StringType()),
    StructField('source', StringType()),
    StructField('keyword', StringType()),
    StructField('applicant', IntegerType()),
    StructField('quota', IntegerType()),
    StructField('requirements', ArrayType(StringType())),
    StructField('skills', ArrayType(StringType())),
    StructField('benefits', ArrayType(StringType())),
    StructField('publisher_name', StringType()),
    StructField('salary', StringType()),
    StructField('publisher_last_online', TimestampType()),
    StructField('listing_date', TimestampType()),
    StructField('scraped_at', TimestampType()),
])

df = spark \
    .read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:19092") \
    .option("subscribe", "rawjobs") \
    .load()
    
jobs = (
        df
        .select(
            col("topic"),
            col("key").cast("string").alias("kafka_key"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("json")
        )
        .select(
            "*",
            from_json(col="json", schema=schema).alias("data")
        )
        .select(
            "topic",
            "kafka_key",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            "data.*"
        )
    )

cleaned_jobs = (
    jobs
    .withColumn("company", trim(lower(col("company"))))
    .withColumn("keyword", trim(lower(col("keyword"))))
    .withColumn("job_name", trim(lower(col("job_name"))))
    .withColumn("ingested_at", current_timestamp())
)

final_job = (
    cleaned_jobs
    .select(
        "company",
        "job_name",
        "url",
        "location",
        "city",
        "province",
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
        "topic",
        "kafka_key",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "publisher_last_online",
        "listing_date",
        "scraped_at",
        "ingested_at"
    )
    .drop_duplicates(["url"])
)
        
query = (
    final_job
    .write
    .format("iceberg")
    .mode("append")
    .save("warehouse.bronze.jobs")
)

print("done ingestion...")
print(f"affected {final_job.count()} rows")
