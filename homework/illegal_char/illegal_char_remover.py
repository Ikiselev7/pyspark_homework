from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, regexp_replace
from typing import List


class IllegalCharRemover:
    def __init__(self, chars: List[str], replacement):
        self.regex = f"[{''.join(chars)}]+"
        self.replacement = replacement

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        return dataframe\
            .withColumn(target_column, regexp_replace(col(source_column), self.regex, self.replacement))\
            .drop(col(source_column))
