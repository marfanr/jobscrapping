from pyspark.sql import SparkSession
from pyspark.sql.functions import pandas_udf, col, row_number, current_timestamp
from pyspark.sql.window import Window
from pyspark.sql.types import LongType

import pandas as pd
import sys
import re

try:
    spark = (
        SparkSession.builder
        .appName("extract_salary")
        .getOrCreate()
    )
    
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