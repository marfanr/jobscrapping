from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("IcebergPolarisTest")
    .getOrCreate()
)

print("\n=== SPARK S3 CONFIG ===")

for key in [
    "spark.sql.catalog.polaris.type",
    "spark.sql.catalog.polaris.uri",
    "spark.sql.catalog.polaris.warehouse",
    "spark.sql.catalog.polaris.io-impl",
    "spark.sql.catalog.polaris.s3.endpoint",
    "spark.sql.catalog.polaris.s3.path-style-access",
    "spark.sql.catalog.polaris.client.region",
]:
    print(f"{key} = {spark.conf.get(key, '<NOT SET>')}")

try:
    print("\n=== CATALOGS ===")
    spark.sql("SHOW CATALOGS").show(truncate=False)

    print("\n=== NAMESPACES ===")
    spark.sql("SHOW NAMESPACES IN polaris").show(truncate=False)

    print("\n=== CREATE NAMESPACE ===")
    spark.sql("""
        CREATE NAMESPACE IF NOT EXISTS polaris.silver
    """)

    print("\n=== CREATE TABLE ===")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS polaris.silver.test_jobs (
            id BIGINT,
            title STRING,
            portal STRING
        )
        USING iceberg
    """)

    print("\n=== INSERT ===")
    spark.sql("""
        INSERT INTO polaris.silver.test_jobs
        VALUES
            (1, 'Backend Developer', 'Jobstreet'),
            (2, 'Data Engineer', 'Glints')
    """)

    print("\n=== SELECT ===")
    spark.sql("""
        SELECT *
        FROM polaris.silver.test_jobs
        ORDER BY id
    """).show(truncate=False)

    print("\n=== TABLES ===")
    spark.sql("""
        SHOW TABLES IN polaris.silver
    """).show(truncate=False)

except Exception as e:
    print(f"ERROR: {e}")
    import traceback
    traceback.print_exc()
    raise

finally:
    spark.stop()