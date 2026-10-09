from pyspark.sql import SparkSession
import sys

try:
    spark = (
        SparkSession.builder
        .appName("debugProcess")
        .getOrCreate()
    )
    
    # spark.sql("SHOW TABLES IN warehouse.silvers").show(truncate=False);
    spark.sql("SELECT * FROM warehouse.bronze.jobs").show(truncate=False)
    spark.sql("SELECT * FROM warehouse.silvers.job_skills_list").show(truncate=False)
    # spark.sql("""
    #     SELECT 
    #         snapshot_id,
    #         committed_at,
    #         operation,
    #         summary['added-records'] AS added_rows,
    #         summary['total-records'] AS total_rows
    #     FROM warehouse.bronze.jobs.snapshots
    #     ORDER BY added_rows DESC
    # """).show(truncate=False)
    
    # spark.sql("SHOW TBLPROPERTIES warehouse.silvers.job_majors_list").show(truncate=False)
    
    spark.sql("SELECT COUNT(*) FROM warehouse.bronze.jobs").show(truncate=False)
    
    latest_bronze_snapshot = spark.sql("SELECT snapshot_id FROM warehouse.bronze.jobs.snapshots ORDER BY committed_at DESC LIMIT 1").take(1)[0]['snapshot_id']
    print(f"latest snaphost : {latest_bronze_snapshot}")
    
    
    

except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise