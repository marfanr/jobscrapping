from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    pandas_udf,
    col,
    current_timestamp
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
        .appName("extract_skills")
        .getOrCreate()
    )
    
    spark.sql("CREATE NAMESPACE IF NOT EXISTS warehouse.silvers")
        
    spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse.silvers.job_skills_list (
        kafka_key STRING,
        skills ARRAY<STRING>,
        updated_at TIMESTAMP
    )
    USING ICEBERG
    """)
    
    skills_list = pd.read_csv("/opt/data/skills.csv")
    target_skills = (
        skills_list['skills']
        .dropna()
        .drop_duplicates()
        .astype(str)
        .tolist()    
    )
    broadcast_keywords = spark.sparkContext.broadcast(target_skills)

    _matcher = None
    _indo_nlp = None

    def get_spacy_phrase_matcher(keywords):
        global _matcher, _indo_nlp
        if _matcher is None and _indo_nlp is None:
            _indo_nlp = Indonesian()
            _matcher = PhraseMatcher(_indo_nlp.vocab, attr="LOWER")
            patterns = [_indo_nlp.make_doc(str(text)) for text in keywords]
            _matcher.add("skills", patterns)
            
        return _indo_nlp, _matcher

    @pandas_udf(ArrayType(StringType()))
    def extract_skills(details: pd.Series) -> pd.Series:
        nlp, m = get_spacy_phrase_matcher(broadcast_keywords.value)
        results = []
        for doc in nlp.pipe(details.fillna("").astype(str), batch_size=256):
            matches = m(doc)
            term = list(
                set(doc[start:end].text.strip() for _, start, end in matches)
            )
            results.append(term)
            
        return pd.Series(results)

    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silvers.job_skills_list")
    props = {row["key"]: row["value"] for row in props_df.collect()}
    last_processed_snapshot = props.get("bronze.last_processed_snapshots_id")
    
    snapshots_rows = spark.sql("""
        SELECT snapshot_id FROM
        warehouse.bronze.jobs.snapshots
        ORDER BY committed_at DESC
        LIMIT 1
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
        raw = (
            spark.read
            .format("iceberg")
            .option("start-snapshot-id", int(last_processed_snapshot))
            .option("end-snapshot-id", int(latest_bronze_snapshot))
            .load("warehouse.bronze.jobs")
        )
    else:
        raw = (
            spark.table("warehouse.bronze.jobs")
        )
    
    sql = (
            raw
            .select("kafka_key", "details", "company")
            .filter(col("kafka_key").isNotNull())
            .drop_duplicates(["kafka_key", "company"])
            .repartition(2)
        )
    
    skills_incoming = (
        sql
        .withColumn("details", extract_skills("details"))
        .select(
            "kafka_key",
            col("details").alias("skills")
        )
        .withColumn("updated_at", current_timestamp())
    )
    
    skills_incoming.createOrReplaceTempView("skills_incoming")
    
    spark.sql(f"""
        ALTER TABLE warehouse.silvers.job_skills_list
        SET TBLPROPERTIES ('bronze.last_processed_snapshots_id'='{latest_bronze_snapshot}')
    """)
    
    spark.sql("""
        MERGE INTO warehouse.silvers.job_skills_list s
        USING skills_incoming m
        ON s.kafka_key = m.kafka_key
        WHEN MATCHED THEN
            UPDATE SET
                s.skills = array_union(
                    coalesce(s.skills, array()),
                    coalesce(m.skills, array())
                ),
                s.updated_at = m.updated_at
        WHEN NOT MATCHED THEN
            INSERT (kafka_key, skills, updated_at)
            VALUES (m.kafka_key, m.skills, m.updated_at)
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