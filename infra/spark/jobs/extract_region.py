from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    pandas_udf,
    col,
    when,
    regexp_replace,
    broadcast,
    coalesce,
    lower,
    trim,
    current_timestamp,
    row_number,
)
from pyspark.sql.window import Window
from pyspark.sql.types import LongType, StringType
from pyspark import StorageLevel
import pandas as pd
import sys
import re
import time
import ahocorasick


COMPACTION_INTERVAL_SECONDS = 1 * 30 * 60  # every 30min

try:
    spark = (
        SparkSession.builder
        .appName("extractCities")
        .getOrCreate()
    )
    sc = spark.sparkContext

    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.silver")

    spark.sql("""
        CREATE TABLE IF NOT EXISTS warehouse.silver.job_region (
            kafka_key STRING,
            city STRING,
            province STRING,
            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (bucket(16, kafka_key))
        TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
        """)

    spark.sql("""
        ALTER TABLE warehouse.silver.job_region
        SET TBLPROPERTIES (
            'format-version'='2',
            'write.merge.mode'='merge-on-read',
            'write.update.mode'='merge-on-read',
            'write.delete.mode'='merge-on-read'
        )
        """)

    try:
        spark.sql("""
            ALTER TABLE warehouse.silver.job_region
            ADD PARTITION FIELD bucket(16, kafka_key)
            """)
    except Exception as e:
        print(f"ADD PARTITION FIELD skipped (likely already applied): {e}")

    villages_df = (
        spark.read.format("iceberg")
        .table("warehouse.seed.seed_villages")
        .select("name", "district_id")
        .dropna()
        .distinct()
    )
    villages_list = [(r[0], r[1]) for r in villages_df.collect() if r[0] and r[1]]

    province_table = (
        spark.read.format("iceberg")
        .table("warehouse.seed.seed_provinces")
        .select(
            col("id").alias("province_id"),
            col("name").alias("province_name")
        )
        .distinct()
    )
    provinces_list = [r[0] for r in province_table.select("province_name").dropna().distinct().collect() if r[0]]

    cities_table = (
        spark.read.format("iceberg")
        .table("warehouse.seed.seed_cities")
        .select(
            col("id").alias("city_id"),
            col("name").alias("city_name"),
            col("provid").alias("city_prov_id")
        )
        .distinct()
    )

    cities_list = [
        str(r[0]).strip() 
        for r in cities_table.select("city_name")
        .dropna().distinct().collect() if r[0]
    ]
    clean_cities_list = [
        re.sub(r'\b([A-Z])\s+(?=[A-Z]\b)', r'\1', c)
        for c in cities_list
    ]
    clean_cities_list = [re.sub(r'^(KOTA|KABUPATEN)\s+', '', c, flags=re.IGNORECASE).strip() for c in clean_cities_list]

    district_table = (
        spark.read.format("iceberg")
        .table("warehouse.seed.seed_districts")
        .select(
            col("id").alias("district_id"),
            col("city_id").alias("district_city_id"),
            col("name").alias("district_name")
        )
        .distinct()
    )
    district_list = [(r[0], r[1]) for r in district_table.select("district_name", "district_city_id").dropna().distinct().collect() if r[0] and r[1]]

    provinces_pattern = r"(?i)\b(" + "|".join([re.escape(r) for r in sorted(provinces_list, key=len, reverse=True)]) + r")\b"

    bc_villages = sc.broadcast(villages_list)                     
    bc_districts = sc.broadcast(district_list)                    
    bc_cities = sc.broadcast([(c, c) for c in clean_cities_list]) 
    bc_provinces = sc.broadcast([(p, p) for p in provinces_list]) 

    def _boundary_ok(before_ch, after_ch):
        return (before_ch is None or not before_ch.isalnum()) and (after_ch is None or not after_ch.isalnum())

    def _best_match(automaton, text_lower):
        best = None  # (start_index, length, value)
        for end_index, (length, value) in automaton.iter(text_lower):
            start_index = end_index - length + 1
            before_ch = text_lower[start_index - 1] if start_index > 0 else None
            after_ch = text_lower[end_index + 1] if end_index + 1 < len(text_lower) else None
            if _boundary_ok(before_ch, after_ch):
                if best is None or start_index < best[0] or (start_index == best[0] and length > best[1]):
                    best = (start_index, length, value)
        return best[2] if best else None

    def make_matcher_udf(bc_pairs, spark_return_type):
        state = {}

        def _get_automaton():
            auto = state.get("automaton")
            if auto is None:
                A = ahocorasick.Automaton()
                for name, value in bc_pairs.value:
                    if name:
                        key = str(name).lower()
                        if key:
                            A.add_word(key, (len(key), value))
                A.make_automaton()
                state["automaton"] = A
                auto = A
            return auto

        @pandas_udf(spark_return_type)
        def _udf(s: pd.Series) -> pd.Series:
            automaton = _get_automaton()
            
            def find(text):
                if pd.isna(text):
                    return None
            
                text = str(text).strip()
                if not text:
                    return None
                return _best_match(automaton, text.lower())

            return s.astype("string").map(find)

        return _udf

    match_village = make_matcher_udf(bc_villages, LongType())
    match_city = make_matcher_udf(bc_cities, StringType())
    match_province = make_matcher_udf(bc_provinces, StringType())
    match_district = make_matcher_udf(bc_districts, LongType())

    # Incremental read from bronze
    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silver.job_region")
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

    raw_jobs = raw_jobs.persist(StorageLevel.MEMORY_AND_DISK)

    jobs_df = (
        raw_jobs
        .select(
            "kafka_key",
            "location",
            col("province").alias("job_province"),
            col("city").alias("job_city")
        )
    )

    jobs_clean_df = (
        jobs_df
        .withColumn("clean_location", regexp_replace(col("location"), provinces_pattern, ""))
    )

    matched_df = (
        jobs_clean_df
        .withColumn("matched_province", 
            when(col("location").isNotNull(), match_province(col("location")))
        )
        .withColumn("matched_province", 
            when(
            col("matched_province").isNull() & col("job_province").isNotNull(),
            match_province(col("job_province")))
            .otherwise(col("matched_province"))
        )
        .withColumn("matched_city", when(col("clean_location").isNotNull(), match_city(col("clean_location"))))
        .withColumn("matched_city", 
            when(col("matched_city").isNull() & col("job_city").isNotNull(),
            match_city(col("job_city")))
            .otherwise(col("matched_city"))
        )
        .withColumn("matched_district_city_id", when(col("clean_location").isNotNull(), match_district(col("clean_location"))))
        .withColumn("matched_district_city_id", 
            when(col("matched_district_city_id").isNull() & col("job_city").isNotNull(),
            match_district(col("job_city")))
            .otherwise(col("matched_district_city_id"))
        )
        .withColumn("matched_village_district_id", when(col("clean_location").isNotNull(), match_village(col("clean_location"))))
        .withColumn("matched_village_district_id", 
            when(col("matched_village_district_id").isNull() & col("job_city").isNotNull(),
            match_village(col("job_city")))
            .otherwise(col("matched_village_district_id"))
        )
    )

    # Pre-alias seed views
    city_by_district = cities_table.alias("c_dist")
    city_by_village = cities_table.alias("c_vil")

    prov_for_dist = province_table.alias("p_dist")
    prov_for_vil = province_table.alias("p_vil")

    enriched_df = (
        matched_df

        .join(
            broadcast(city_by_district),
            matched_df.matched_district_city_id == col("c_dist.city_id"),
            "left"
        )
        .join(
            broadcast(prov_for_dist),
            col("c_dist.city_prov_id") == col("p_dist.province_id"),
            "left"
        )
        .join(
            broadcast(district_table.alias("d_vil")),
            matched_df.matched_village_district_id == col("d_vil.district_id"),
            "left"
        )
        .join(
            broadcast(city_by_village),
            col("d_vil.district_city_id") == col("c_vil.city_id"),
            "left"
        )
        .join(
            broadcast(prov_for_vil),
            col("c_vil.city_prov_id") == col("p_vil.province_id"),
            "left"
        )
    )

    final_province_expr = coalesce(
        col("matched_province"),
        col("p_dist.province_name"),
        col("p_vil.province_name")
    )

    is_dist_province_valid = (
        col("c_dist.city_name").isNotNull() &
        (
            col("matched_province").isNull() |
            (lower(trim(col("p_dist.province_name"))) == lower(trim(col("matched_province"))))
        )
    )

    final_city_expr = coalesce(
        col("matched_city"),
        when(is_dist_province_valid, col("c_dist.city_name")),
        col("c_vil.city_name")
    )

    window_spec = Window.partitionBy("kafka_key").orderBy(col("now").desc())
    incoming_region_df = (
        enriched_df
        .withColumn("final_province", final_province_expr)
        .withColumn("final_city", final_city_expr)
        .select(
            "kafka_key",
            lower("final_city").alias("city"),
            lower("final_province").alias("province")
        )
        .withColumn("now", current_timestamp())
        .withColumn("rn", row_number().over(window_spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )

    incoming_region_df = incoming_region_df.persist(StorageLevel.MEMORY_AND_DISK)
    incoming_region_df.show(20, truncate=False)
    incoming_region_df.createOrReplaceTempView("incoming_region")

    t0 = time.time()
    spark.sql("""
    MERGE INTO warehouse.silver.job_region j
    USING incoming_region i
    ON
        j.kafka_key = i.kafka_key
    WHEN MATCHED THEN
        UPDATE SET
            j.city = i.city,
            j.province = i.province,
            j.updated_at = i.now
    WHEN NOT MATCHED THEN
        INSERT (kafka_key, city, province, updated_at)
        VALUES (i.kafka_key, i.city, i.province, i.now)    
    """)
    print(f"MERGE took {time.time() - t0:.2f}s")

    spark.sql(f"""
        ALTER TABLE warehouse.silver.job_region
        SET TBLPROPERTIES ('bronze.last_processed_snapshot_id'='{latest_bronze_snapshot}')
    """)

    print(f"affected {raw_jobs.count()} rows")

    
    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silver.job_region")
    props = {row["key"]: row["value"] for row in props_df.collect()}
    last_compaction_ts = float(props.get("silver.last_compaction_ts", "0"))

    if time.time() - last_compaction_ts > COMPACTION_INTERVAL_SECONDS:
        print("Running compaction (rewrite_data_files + rewrite_position_delete_files)...")
        t0 = time.time()
        spark.sql("CALL warehouse.system.rewrite_data_files(table => 'silver.job_region')")
        spark.sql("CALL warehouse.system.rewrite_position_delete_files(table => 'silver.job_region')")
        print(f"Compaction took {time.time() - t0:.2f}s")

        spark.sql(f"""
            ALTER TABLE warehouse.silver.job_region
            SET TBLPROPERTIES ('silver.last_compaction_ts'='{time.time()}')
        """)
    else:
        next_in = COMPACTION_INTERVAL_SECONDS - (time.time() - last_compaction_ts)
        print(f"Skipping compaction, next due in {next_in / 60:.1f} min")

    incoming_region_df.unpersist()
    raw_jobs.unpersist()
    bc_villages.unpersist()
    bc_districts.unpersist()
    bc_cities.unpersist()
    bc_provinces.unpersist()


except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    import traceback
    traceback.print_exc(file=sys.stderr)
    sys.stderr.flush()
    raise