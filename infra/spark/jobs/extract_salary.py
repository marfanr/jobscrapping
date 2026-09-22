from pyspark.sql import SparkSession, Row
import sys

try:
    spark = (
        SparkSession.builder
        .appName("extract_salary")
        .getOrCreate()
    )
    
    sql = (
        spark.sql("""
            SELECT kafka_key, salary 
            FROM warehouse.bronze.jobs
        """)
        
    )
    
    # log = Row(
    #     batch_id=batch_id,
    #     source=source,
    #     started_at=started_at,
    #     finished_at=finished_at,
    #     status="SUCCESS",
    #     row_count=row_count,
    #     error_message=None
    # )

    # df = spark.createDataFrame([log])

    # df.writeTo(
    #     "warehouse.meta.processing_log"
    # ).append()
    
except Exception as e:
    print(
        f"err: {type(e).__name__}: {e}",
        file=sys.stderr,
        flush=True
    )

    import traceback
    traceback.print_exc(file=sys.stderr)

    raise