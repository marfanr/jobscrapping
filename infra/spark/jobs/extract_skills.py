from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import (
    pandas_udf,
    col,    
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

    sql = (
        spark.sql("SELECT kafka_key, details FROM warehouse.bronze.jobs")
            .withColumn("details", extract_skills("details"))
            .select(
                "details"
            )
        )
    sql.show(truncate=False)
    
    # log = Row(
    #     batch_id=batch_id,
    #     source=source,
    #     started_at=started_at,
    #     finished_at=finished_at,
    #     status="SUCCESS",
    #     row_count=row_count,
    #     error_message=None
    # )

    # df = spark.createDataFrame([log])

    # df.writeTo(
    #     "warehouse.meta.processing_log"
    # ).append()
    
except Exception as e:
    print(
        f"err: {type(e).__name__}: {e}",
        file=sys.stderr,
        flush=True
    )

    import traceback
    traceback.print_exc(file=sys.stderr)

    raise