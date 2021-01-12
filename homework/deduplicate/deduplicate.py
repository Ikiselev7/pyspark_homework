from typing import Union, List

from pyspark.sql import DataFrame


class Deduplicator:
    """
    Current class provides possibilities to remove duplicated rows in data depends on provided primary key.
    If no primary keys were provided should be removed only identical rows.
    """

    # ToDo: Implement this method
    def deduplicate(self, primary_keys: Union[str, List[str]], dataframe: DataFrame) -> DataFrame:
        if primary_keys:
            if isinstance(primary_keys, str):
                primary_keys = [primary_keys]
            df_modified = dataframe.dropDuplicates(primary_keys)
        else:
            df_modified = dataframe.distinct()
        return df_modified
