from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import (
    now
)

try:
    spark = (
        SparkSession.builder
        .appName("initial")
        .getOrCreate()
    )
    
    # spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.meta")
    # spark.sql("""
    #     CREATE TABLE IF NOT EXISTS warehouse.meta.processing_log (
    #         batch_id STRING,
    #         source STRING,
    #         started_at TIMESTAMP,
    #         finished_at TIMESTAMP,
    #         status STRING,
    #         row_count BIGINT,
    #         error_message STRING
    #     ) 
    #     USING ICEBERG
    # """)
    
    
except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise