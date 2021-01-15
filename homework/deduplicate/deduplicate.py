from typing import Union, List

from pyspark.sql import DataFrame


class Deduplicator:
    """
    Current class provides possibilities to remove duplicated rows in data depends on provided primary key.
    If no primary keys were provided should be removed only identical rows.
    """

    def deduplicate(self, primary_keys: Union[str, List[str]], dataframe: DataFrame) -> DataFrame:
        primary_keys = [primary_keys] if isinstance(primary_keys, str) else primary_keys
        return dataframe.drop_duplicates(primary_keys or None)
