from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    pandas_udf,
    col,
    current_timestamp,
    broadcast,
    expr
)
from pyspark.sql.types import (
    ArrayType,
    StringType
)
import pandas as pd
import sys

try:
    spark = (
        SparkSession.builder
        .appName("extractCitties")
        .getOrCreate()
    )
    
    # spark.sql("""
    #     CREATE TABLE IF NOT EXISTS warehouse.silver.cities (
    #         kafka_key STRING,
    #         majors ARRAY<STRING>,
    #         updated_at TIMESTAMP
    #     )
    #     USING ICEBERG
    #     """)
    
    
    villages_df = (
        spark
        .read
        .format("iceberg")
        .table("warehouse.seed.seed_villages")
        .select(col("name").alias("village"))
        .repartition(2)
    )
    
    cities_df = (
            spark
            .read
            .format("iceberg")
            .table("warehouse.seed.seed_cities")
            .select(col("name").alias("city"))
            .repartition(2)
        )
    
    jobs_df = (
        spark
        .read
        .format("iceberg")
        .table("warehouse.bronze.jobs")
        .select(
            "kafka_key",
            "location",
            col("city").alias("job_city")
        )
    )
    
    results_df = (
        jobs_df
        .join(
            broadcast(cities_df),
            expr(
                "LOWER(job_city) LIKE LOWER(CONCAT('%', city, '%'))"
                "OR LOWER(location) LIKE LOWER(CONCAT('%', city, '%'))"
            ),
            "left"
        )
        .where(
            col("city").isNull()
        )
    )
    
    results_df.show(n=5)
    
    unmatched_df = (
        results_df
        .join(
            broadcast(villages_df),
            expr(
                "LOWER(job_city) LIKE LOWER(CONCAT('%', village, '%'))"
                "OR LOWER(location) LIKE LOWER(CONCAT('%', village, '%'))"
            ),
            "left"
        )
    )
    unmatched_df.show()
        
    
except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise