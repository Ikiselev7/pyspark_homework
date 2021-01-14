from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import regexp_replace

from typing import List


class IllegalCharRemover:
    """
    Class provides possibilities to remove illegal chars from string column.
    """
    def __init__(self, chars: List[str], replacement):
        print(chars, replacement)

        if chars:
            self.chars = chars
        else:
            raise ValueError

        if replacement is not None:
            self.replacement = replacement
        else:
            raise ValueError

    def remove_illegal_chars(self, dataframe: DataFrame, source_column: str, target_column: str):
        escapes = ['\\','.','+','*','?','[','^',']','$','(',')','{','}','=','!','<','>','|',':']
        chars = ['\\'+char if char in escapes else char for char in self.chars]
        print(chars)
        chars = ''.join(chars)
        dataframe = dataframe\
            .withColumn(source_column, regexp_replace(source_column, fr'[{chars}]', self.replacement))\
            .withColumnRenamed(source_column, target_column)

        for d in dataframe.collect():
            print(d)

        return dataframe
