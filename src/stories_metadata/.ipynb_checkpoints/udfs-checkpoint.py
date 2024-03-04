import math
from datetime import date

from pyspark.sql.functions import udf

import langid


@udf
def lang_detect(content: str) -> str:
    # langid.classify() -> (ISO 639-1 code, accuracy score)
    # https://github.com/saffsd/langid.py
    return langid.classify(content)[0]
