from typing import Union, List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

class Deduplicator:
    """
    Current class provides possibilities to remove duplicated rows in data depends on provided primary key.
    If no primary keys were provided should be removed only identical rows.
    """

    # ToDo: Implement this method
    def deduplicate(self, primary_keys: Union[str, List[str]], dataframe: DataFrame) -> DataFrame:
        if not primary_keys:
            dataframe = dataframe.dropDuplicates()
        elif isinstance(primary_keys, list):
            dataframe = dataframe.dropDuplicates(primary_keys)
        else:
            dataframe = dataframe.dropDuplicates([primary_keys])
        return dataframe