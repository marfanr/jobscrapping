from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    pandas_udf,
    col,
    current_timestamp,
    expr,
    array_distinct,
    flatten,
    collect_list
)
from pyspark.sql.types import (
    ArrayType,
    StringType
)
import sys
import pandas as pd
from spacy.matcher import PhraseMatcher
from spacy.lang.id import Indonesian

try:
    spark = (
        SparkSession.builder
        .appName("extract_majors")
        .getOrCreate()
    )
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.silvers")
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse.silvers.job_majors_list (
        kafka_key STRING,
        majors ARRAY<STRING>,
        updated_at TIMESTAMP
    )
    USING ICEBERG
    """)
    
    majors_list = pd.read_csv("/opt/data/majors.csv")
    target_majors = (
        majors_list['majors']
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()    
    )
    broadcast_keywords = spark.sparkContext.broadcast(target_majors)

    _matcher = None
    _indo_nlp = None

    def get_spacy_phrase_matcher(keywords):
        global _matcher, _indo_nlp
        if _matcher is None and _indo_nlp is None:
            _indo_nlp = Indonesian()
            _matcher = PhraseMatcher(_indo_nlp.vocab, attr="LOWER")
            patterns = [_indo_nlp.make_doc(str(text)) for text in keywords]
            _matcher.add("majors", patterns)
            
        return _indo_nlp, _matcher

    @pandas_udf(ArrayType(StringType()))
    def extract_majors(details: pd.Series) -> pd.Series:
        nlp, m = get_spacy_phrase_matcher(broadcast_keywords.value)
        results = []
        for doc in nlp.pipe(details.fillna("").astype(str), batch_size=256):
            matches = m(doc)
            term = list(
                set(doc[start:end].text.strip() for _, start, end in matches)
            )
            results.append(term)
            
        return pd.Series(results)
    
    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silvers.job_majors_list")
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
        .select("kafka_key", "details", "company")
        .filter(col("kafka_key").isNotNull())
        .dropDuplicates(["kafka_key", "company"])
        .repartition(2)
    )
    
    majors_incoming = (
        sql
        .withColumn("details", extract_majors("details"))
        .select(
            "kafka_key",
            col("details").alias("majors"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    
    majors_incoming.createOrReplaceTempView("incoming_majors")
    
    
    spark.sql(f"""
        ALTER TABLE warehouse.silvers.job_majors_list
        SET TBLPROPERTIES ('bronze.last_processed_snapshot_id'='{latest_bronze_snapshot}')
    """)
    
    
    spark.sql("""
        MERGE INTO warehouse.silvers.job_majors_list m
        USING incoming_majors i
        ON m.kafka_key = i.kafka_key
        WHEN MATCHED THEN
            UPDATE SET
                m.majors = array_union(
                    coalesce(m.majors, array()),
                    coalesce(i.majors, array())
                ),
                m.updated_at = i.updated_at
        WHEN NOT MATCHED THEN
            INSERT (kafka_key, majors, updated_at)
            VALUES (i.kafka_key, i.majors, i.updated_at)
        """)

    print(f"processed {sql.count()} data")
        
except Exception as e:
    print(
        f"err: {type(e).__name__}: {e}",
        file=sys.stderr,
        flush=True
    )

    import traceback
    traceback.print_exc(file=sys.stderr)

    raise