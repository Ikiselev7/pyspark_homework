from pyspark.sql.dataframe import DataFrame
from typing import List
from pyspark.sql.functions import regexp_replace


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        if not chars or replacement is None:
            raise ValueError
        self.replacement = replacement
        self.chars = chars

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        chars = self.chars
        replacement = self.replacement
        updated_df = dataframe.withColumn(target_column, dataframe[source_column])

        for char in chars:
            char = char if char.isalnum() else f'\{char}'
            updated_df = updated_df.withColumn(target_column,
                                               regexp_replace(target_column, char, replacement))

        return updated_df.drop(source_column)
