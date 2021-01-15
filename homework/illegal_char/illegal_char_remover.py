from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_replace
from typing import List
import re


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """

    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError

        chars = [c if c not in r'-\^[]' else re.escape(c) for c in chars]

        self.chars = f'[{"".join(chars)}]+'
        self.replacement = replacement

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        return dataframe \
            .withColumn(target_column, regexp_replace(source_column, self.chars, self.replacement)) \
            .drop(dataframe[source_column])
