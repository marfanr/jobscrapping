import sys
import traceback

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, pandas_udf
from pyspark.sql.types import StringType
from spacy.lang.xx import MultiLanguage
from spacy.matcher import Matcher

PRIORITY = {"senior": 0, "mid_level": 1, "entry_level": 2}

NUM = {"LOWER": {"REGEX": (
    r"^(\d+|one|two|three|four|five|six|seven|eight|nine|ten"
    r"|satu|dua|tiga|empat|lima|enam|tujuh|delapan|sembilan|sepuluh)$"
)}}
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
         {"LOWER": {"IN": ["graduate", "graduates", "grad", "grads"]}}],
        [{"LOWER": {"IN": ["freshgrad", "freshgraduate", "fresh-graduate"]}}],
        [{"LOWER": "entry"}, {"TEXT": "-", "OP": "?"}, {"LOWER": "level"}],
        [{"LOWER": "no"}, {"LOWER": {"IN": ["prior", "previous", "relevant"]}, "OP": "?"},
         {"LOWER": "experience"}],
    ],
    "mid_level": [
        [NUM, RANGE_SEP, NUM, YEARS],
        [NUM, {"LOWER": "s"}, {"TEXT": "/"}, {"LOWER": "d"}, NUM, YEARS],
        [{"TEXT": {"REGEX": r"^\d+\s*[-–]\s*\d+$"}}, YEARS],
        [{"LOWER": "pengalaman"}, NUM, YEARS],
        [NUM, YEARS, OF_OPT, EXPERIENCE],
    ],
    "senior": [
        [NUM, {"TEXT": "+"}, YEARS],
        [{"TEXT": {"REGEX": r"^\d+\+$"}}, YEARS],  
        [{"LOWER": {"IN": ["minimal", "minimum", "min"]}}, NUM, YEARS],
        [{"LOWER": "at"}, {"LOWER": "least"}, NUM, YEARS],
        [{"LOWER": "more"}, {"LOWER": "than"}, NUM, YEARS],
        [{"LOWER": {"IN": ["over", "lebih"]}}, {"LOWER": {"IN": ["dari"]}, "OP": "?"}, NUM, YEARS],
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


@pandas_udf(StringType())
def detect_seniority(details: pd.Series) -> pd.Series:
    nlp, matcher = get_nlp_and_matcher()
    vocab = nlp.vocab
    results = []
    for doc in nlp.tokenizer.pipe(details.fillna("").astype(str), batch_size=1000):
        labels = {vocab.strings[match_id] for match_id, _, _ in matcher(doc)}
        results.append(min(labels, key=PRIORITY.get) if labels else None)
    return pd.Series(results)


try:
    spark = SparkSession.builder.appName("extract_seniority").getOrCreate()

    raw_jobs = spark.table("warehouse.bronze.jobs")

    df = (
        raw_jobs
        .select("kafka_key", "details")
        .withColumn("seniority", detect_seniority(col("details")))
    )

    df.show()

except Exception as e:
    print(f"err: {type(e).__name__}: {e}", file=sys.stderr, flush=True)
    traceback.print_exc(file=sys.stderr)
    raise