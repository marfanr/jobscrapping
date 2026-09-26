from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

import pandas as pd
import sys
import re

try:
    spark = (
        SparkSession.builder
        .appName("extract_salary")
        .getOrCreate()
    )
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.silver")
    
    spark.sql("""
        CREATE TABLE IF NOT EXISTS warehouse.silver.job_salary (
            kafka_key STRING,
            raw_salary STRING,
            source STRING,
            lower_salary BIGINT,
            upper_salary BIGINT,
            updated_at TIMESTAMP
        )
        USING ICEBERG
    """)
    
    # Incremental read from bronze
    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silver.job_salary")
    props = {row["key"]: row["value"] for row in props_df.collect()}
    last_processed_snapshot = props.get("bronze.last_processed_snapshot_id")

    snapshots_rows = spark.sql("""
        SELECT snapshot_id FROM warehouse.bronze.jobs.snapshots
        ORDER BY committed_at DESC LIMIT 1
    """).collect()

    if not snapshots_rows:
        print("Bronze table has no snapshots yet. Exiting.")
        sys.exit(0)

    latest_bronze_snapshot = str(snapshots_rows[0]['snapshot_id'])

    print(f"current processed snapshot: {last_processed_snapshot}")
    print(f"current bronze snapshot: {latest_bronze_snapshot}")

    if last_processed_snapshot == latest_bronze_snapshot:
        print(f"No new snapshots found in Bronze ({latest_bronze_snapshot}). Nothing to process. Exiting.")
        sys.exit(0)

    if last_processed_snapshot:
        raw_jobs = (
            spark.read
            .format("iceberg")
            .option("start-snapshot-id", int(last_processed_snapshot))
            .option("end-snapshot-id", int(latest_bronze_snapshot))
            .load("warehouse.bronze.jobs")
        )
    else:
        raw_jobs = spark.table("warehouse.bronze.jobs")

    sql = (
        raw_jobs
        .select(
            "kafka_key",
            "salary",
            "source"
        )
    )

    @pandas_udf(LongType())
    def extract_lower_salary(
        salary: pd.Series,
        source: pd.Series
    ) -> pd.Series:

        def parse(value, source_value):

            if pd.isna(value) or pd.isna(source_value):
                return None

            s_ = str(source_value).lower()
            if s_ == "jobstreet":
                salary_value = str(value).strip()

                formatted_salarys = re.findall(
                    r"\d[\d.]*",
                    salary_value
                )

                if not formatted_salarys:
                    return None

                return int(
                    formatted_salarys[0].replace(".", "")
                )

        return pd.Series(
            [
                parse(value, source_value)
                for value, source_value
                in zip(salary, source)
            ]
        )


    @pandas_udf(LongType())
    def extract_upper_salary(
        salary: pd.Series,
        source: pd.Series
    ) -> pd.Series:

        def parse(value, source_value):

            if pd.isna(value) or pd.isna(source_value):
                return None

            s_ = str(source_value).lower()
            if s_ == "jobstreet":
                salary_value = str(value).strip()

                formatted_salarys = re.findall(
                    r"\d[\d.]*",
                    salary_value
                )

                if len(formatted_salarys) < 2:
                    return None

                return int(
                    formatted_salarys[1].replace(".", "")
                )

        return pd.Series(
            [
                parse(value, source_value)
                for value, source_value
                in zip(salary, source)
            ]
        )


    window = Window.partitionBy(col("now").desc())
    incoming = (
        sql
        .withColumn(
            "lower_salary",
            extract_lower_salary(
                col("salary"),
                col("source")
            )
        )
        .withColumn(
            "upper_salary",
            extract_upper_salary(
                col("salary"),
                col("source")
            )
        )
        .withColumn(
            "now",
            current_timestamp()
        )
        .withColumn("rn", row_number().over(window))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    incoming.createOrReplaceTempView("incoming_salary")
    
    incoming.show(
        truncate=False
    )
    
    spark.sql(
    """
    MERGE INTO warehouse.silver.job_salary j
    USING incoming_salary s
    ON 
        j.kafka_key = s.kafka_key
    WHEN MATCHED THEN
        UPDATE SET
            j.raw_salary = s.salary,
            j.source = s.source,
            j.lower_salary = s.lower_salary,
            j.upper_salary = s.upper_salary,
            j.updated_at = s.now
    WHEN NOT MATCHED THEN
        INSERT (kafka_key, raw_salary, source, lower_salary, upper_salary, updated_at)
        VALUES (s.kafka_key, s.salary, s.source, s.lower_salary, s.upper_salary, s.now)
    """
    )
    
    spark.sql(f"""
    ALTER TABLE warehouse.silver.job_salary
    SET TBLPROPERTIES ('bronze.last_processed_snapshot_id'='{latest_bronze_snapshot}')
    """)
    
    print(f"processed {sql.count()} rows")


except Exception as e:
    print(
        f"err: {type(e).__name__}: {e}",
        file=sys.stderr,
        flush=True
    )

    import traceback

    traceback.print_exc(
        file=sys.stderr
    )

    raise