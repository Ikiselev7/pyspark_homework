from typing import List

from pyspark.sql import DataFrame


class Unpivot:
    """
    Class provides unpivoting of some columns in dataset.
    For example for next dataset:
    +---+-----+-----+-----+-----+
    | id| name|10.02|20.02|28.02|
    +---+-----+-----+-----+-----+
    |  1| Ivan|  0.1|  0.1|  0.7|
    |  2|Maria|  0.2|  0.5|  0.9|
    +---+-----+-----+-----+-----+

    if we will consider `id` and `name` as constant columns, and columns 10.02, 20.02, 28.02 as dates,
    and other values as score it should provide next result:

    +---+-----+-----+-----+
    | id| name| date|score|
    +---+-----+-----+-----+
    |  1| Ivan|10.02|  0.1|
    |  1| Ivan|28.02|  0.7|
    |  1| Ivan|20.02|  0.1|
    |  2|Maria|10.02|  0.2|
    |  2|Maria|28.02|  0.9|
    |  2|Maria|20.02|  0.5|
    +---+-----+-----+-----+

    See spark sql function `stack`.
    """

    def __init__(self, constant_columns: List[str], key_col='', value_col=''):
        self.constant_columns = constant_columns
        self.key_col = key_col
        self.value_col = value_col

    def unpivot(self, dataframe: DataFrame) -> DataFrame:
        if not self.key_col or not self.value_col:
            return dataframe

        cols_to_unpivot = [c for c in dataframe.columns if c not in self.constant_columns]
        stack_body = ', '.join([f"'{c}', `{c}`" for c in cols_to_unpivot])

        return dataframe.selectExpr(
            *self.constant_columns,
            f"stack({len(cols_to_unpivot)}, {stack_body}) as ({self.key_col}, {self.value_col})"
        )
