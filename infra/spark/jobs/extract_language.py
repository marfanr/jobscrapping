from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, when, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import StringType, ArrayType
import pandas as pd
import sys
from spacy.matcher import PhraseMatcher
from spacy.lang.id import Indonesian

"""
This jobs aim to detect is the job require some language
"""

TARGET_LANGUAGE = [
    "english",
    "japanese",
    "mandarin"
]

try:
    spark = (
        SparkSession.builder
        .appName("extract_salary")
        .getOrCreate()
    )
    
    spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse.silver.job_language_list (
        kafka_key STRING,
        languages ARRAY<STRING>,
        updated_at TIMESTAMP
    )
    USING ICEBERG          
    """)
    
    broadcast_keywords = spark.sparkContext.broadcast(set(TARGET_LANGUAGE))
    
    _matcher = None
    _indo_nlp = None

    def get_spacy_phrase_matcher(keywords):
        global _matcher, _indo_nlp
        if _matcher is None and _indo_nlp is None:
            _indo_nlp = Indonesian()
            _matcher = PhraseMatcher(_indo_nlp.vocab, attr="LOWER")
            patterns = [_indo_nlp.make_doc(str(text)) for text in keywords]
            _matcher.add("lang", patterns)
            
        return _indo_nlp, _matcher
    
    @pandas_udf(ArrayType(StringType()))
    def detect_Language(df: pd.Series) -> pd.Series:
        nlp, m = get_spacy_phrase_matcher(broadcast_keywords.value)
        results = []
        for doc in nlp.pipe(str.lower(df.fillna("").astype(str)), batch_size=256):
            matches  = m(doc)
            term = list(set(
                doc[s:e].text.strip() for _, s, e in matches
            ))
            results.append(term)
        
        return pd.Series(results)

    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silver.job_language_list")
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
            "details"
        )
    )
    
    spec = Window.partitionBy(col("kafka_key")).orderBy(col("now").desc())
    incoming_lang = (
        sql
        .withColumn("languages", 
            when(col("details").isNotNull() ,detect_Language(col("details")))
            .otherwise(None)
        )
        .withColumn("now", current_timestamp())
        .withColumn("rn", row_number().over(spec))
        .filter(col("rn") == 1)
        .drop("rn")
    )
    
    incoming_lang.show()
    incoming_lang.createOrReplaceTempView("incoming_lang")

    spark.sql("""
    MERGE INTO warehouse.silver.job_language_list j
    USING incoming_lang i
    ON
        j.kafka_key = i.kafka_key
    WHEN MATCHED THEN
        UPDATE SET
            j.languages = array_union(
                colaesce(j.languages, array()),
                colaesce(i.languages, array())
            ),
            j.updated_at = i.now
    WHEN NOT MATCHED THEN
        INSERT *
        VALUE (i.kafka_key, i.languages, i.now)
    """)
    
    spark.sql(f"""
        ALTER TABLE warehouse.silver.job_language_list
        SET TBLPROPERTIES ('bronze.last_processed_snapshot_id'='{latest_bronze_snapshot}')
    """)
    
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