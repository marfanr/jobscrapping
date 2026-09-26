from pyspark.sql import SparkSession
import sys
from pyspark.sql.functions import (
    now
)
import sys

try:
    spark = (
        SparkSession.builder
        .appName("initial")
        .getOrCreate()
    )
    
    if spark.catalog.tableExists("warehouse.seed.seed_villages"):
        spark.stop()        
        sys.exit(0)
        
    
    csv = (
        spark
        .read
        .option("header", "true")
        .option("inferSchema", "true")
        .option("ignoreTrailingWhitespace", "true")
        .option("ignoreLeadingWhitespace", "true")
        .csv("/opt/data/seed_villages.csv")
    )
    
    csv \
    .write \
    .format("iceberg") \
    .saveAsTable("warehouse.seed.seed_villages")
    
    spark.stop()
    
    
except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise