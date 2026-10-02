import re
import sys
import traceback

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, pandas_udf, row_number
from pyspark.sql.types import StringType
from pyspark.sql.window import Window
from spacy.lang.xx import MultiLanguage
from spacy.matcher import Matcher

ORDER_COL = "kafka_timestamp"
SENIOR_MIN_YEARS = 5
MID_MIN_YEARS = 2

PRIORITY = {"senior": 0, "mid_level": 1, "entry_level": 2}

NUM_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5,
    "six": 6, "seven": 7, "eight": 8, "nine": 9, "ten": 10,
    "satu": 1, "dua": 2, "tiga": 3, "empat": 4, "lima": 5,
    "enam": 6, "tujuh": 7, "delapan": 8, "sembilan": 9, "sepuluh": 10,
}

NUM = {"LOWER": {"REGEX": r"^(\d+|" + "|".join(NUM_WORDS) + r")$"}}
YEARS = {"LOWER": {"IN": ["tahun", "thn", "th", "year", "years", "yr", "yrs"]}}
RANGE_SEP = {"LOWER": {"IN": ["-", "–", "—", "s/d", "sd", "sampai", "hingga", "to", "and"]}}
OF_OPT = {"LOWER": "of", "OP": "?"}
EXPERIENCE = {"LOWER": {"IN": ["experience", "pengalaman"]}}

PATTERNS = {
    "entry_level": [
        [{"LOWER": "lulusan"}, {"LOWER": "baru"}],
        [{"LOWER": "tanpa"}, {"LOWER": "pengalaman"}],
        [{"LOWER": "belum"}, {"LOWER": "berpengalaman"}],
        [{"LOWER": {"IN": ["fresh", "recent", "new"]}},
         {"TEXT": "-", "OP": "?"},
         {"LOWER": {"IN": ["graduate", "graduates", "grad", "grads"]}}],
        [{"LOWER": {"IN": ["freshgrad", "freshgraduate"]}}],
        [{"LOWER": "entry"}, {"TEXT": "-", "OP": "?"}, {"LOWER": "level"}],
        [{"LOWER": "no"}, {"LOWER": {"IN": ["prior", "previous", "relevant"]}, "OP": "?"},
         {"LOWER": "experience"}],
        [{"LOWER": {"IN": ["intern", "internship", "magang"]}}],
    ],
    
    "years": [
        [NUM, RANGE_SEP, NUM, YEARS],
        [NUM, {"LOWER": "s"}, {"TEXT": "/"}, {"LOWER": "d"}, NUM, YEARS],
        [{"TEXT": {"REGEX": r"^\d+\s*[-–]\s*\d+$"}}, YEARS],
        [{"LOWER": "pengalaman"}, NUM, YEARS],
        [NUM, YEARS, OF_OPT, EXPERIENCE],
        [NUM, {"TEXT": "+"}, YEARS],
        [{"TEXT": {"REGEX": r"^\d+\+$"}}, YEARS],
        [{"LOWER": {"IN": ["minimal", "minimum", "min"]}}, NUM, YEARS],
        [{"LOWER": "at"}, {"LOWER": "least"}, NUM, YEARS],
        [{"LOWER": "more"}, {"LOWER": "than"}, NUM, YEARS],
        [{"LOWER": {"IN": ["over", "lebih"]}}, {"LOWER": "dari", "OP": "?"}, NUM, YEARS],
    ],
}

_nlp = None
_matcher = None


def get_nlp_and_matcher():
    global _nlp, _matcher
    if _nlp is None:
        _nlp = MultiLanguage()
        _matcher = Matcher(_nlp.vocab)
        for label, patterns in PATTERNS.items():
            _matcher.add(label, patterns)
    return _nlp, _matcher


def _numbers(span):
    out = []
    for t in span:
        digits = re.findall(r"\d+", t.text)
        if digits:
            out.extend(int(d) for d in digits)
        elif t.lower_ in NUM_WORDS:
            out.append(NUM_WORDS[t.lower_])
    return out


def _years_to_label(years):
    if years >= SENIOR_MIN_YEARS:
        return "senior"
    if years >= MID_MIN_YEARS:
        return "mid_level"
    return "entry_level"


@pandas_udf(StringType())
def detect_seniority(details: pd.Series) -> pd.Series:
    nlp, matcher = get_nlp_and_matcher()
    vocab = nlp.vocab
    results = []
    for doc in nlp.tokenizer.pipe(details.fillna("").astype(str), batch_size=1000):
        labels = set()
        years = []
        for match_id, start, end in matcher(doc):
            label = vocab.strings[match_id]
            if label == "years":
                nums = _numbers(doc[start:end])
                if nums:
                    years.append(min(nums))  
            else:
                labels.add(label)
        if years:
            labels.add(_years_to_label(max(years)))
        results.append(min(labels, key=PRIORITY.get) if labels else None)
    return pd.Series(results)


df = None
try:
    spark = SparkSession.builder.appName("extract_seniority").getOrCreate()

    spark.sql("""
    CREATE TABLE IF NOT EXISTS warehouse.silver.job_seniority (
        kafka_key STRING,
        seniority STRING,
        updated_at TIMESTAMP
    )
    USING ICEBERG
    TBLPROPERTIES (
        'format-version'='2'
    )
    """)

    props_df = spark.sql("SHOW TBLPROPERTIES warehouse.silver.job_seniority")
    props = {row["key"]: row["value"] for row in props_df.collect()}
    last_processed_snapshot = props.get("bronze.last_processed_snapshot_id")

    ref_rows = spark.sql("""
        SELECT snapshot_id FROM warehouse.bronze.jobs.refs WHERE name = 'main'
    """).collect()
    if not ref_rows:
        print("Bronze table has no snapshots. Exiting.")
        sys.exit(0)
    latest_bronze_snapshot = str(ref_rows[0]["snapshot_id"])

    print(f"last_processed_snapshot: {last_processed_snapshot}")
    print(f"latest_bronze_snapshot: {latest_bronze_snapshot}")

    if last_processed_snapshot == latest_bronze_snapshot:
        print("No new snapshot to process. Exiting.")
        sys.exit(0)

    if last_processed_snapshot:
        print(f"Processing new snapshot: {latest_bronze_snapshot}")
        raw_jobs = (
            spark.read
            .format("iceberg")
            .option("start-snapshot-id", last_processed_snapshot)
            .option("end-snapshot-id", latest_bronze_snapshot)
            .load("warehouse.bronze.jobs")
        )
    else:
        raw_jobs = spark.table("warehouse.bronze.jobs")

    latest_per_key = (
        raw_jobs
        .select("kafka_key", "details", ORDER_COL)
        .filter(col("kafka_key").isNotNull())
        .withColumn(
            "row_num",
            row_number().over(Window.partitionBy("kafka_key").orderBy(col(ORDER_COL).desc())),
        )
        .filter(col("row_num") == 1)
        .drop("row_num")
    )

    df = (
        latest_per_key
        .withColumn("seniority", detect_seniority(col("details")))
        .withColumn("updated_at", current_timestamp())
        .select("kafka_key", "seniority", "updated_at")
    )

    n_rows = df.count()  
    df.show()
    df.createOrReplaceTempView("incoming_seniority")

    spark.sql("""
        MERGE INTO warehouse.silver.job_seniority j
        USING incoming_seniority i
        ON
            j.kafka_key = i.kafka_key
        WHEN MATCHED THEN
            UPDATE SET
                j.seniority = i.seniority,
                j.updated_at = i.updated_at
        WHEN NOT MATCHED THEN
            INSERT (kafka_key, seniority, updated_at)
            VALUES (i.kafka_key, i.seniority, i.updated_at)
        """)

    # spark.sql(f"""
    # ALTER TABLE warehouse.silver.job_seniority
    # SET TBLPROPERTIES (
    #     'bronze.last_processed_snapshot_id' = '{latest_bronze_snapshot}'
    # )
    # """)

    # print(f"upserted rows: {n_rows}")

except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    raise