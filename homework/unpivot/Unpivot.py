from typing import List
from pyspark.sql.functions import expr
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

    # ToDo: implement unpivot transformation
    def unpivot(self, dataframe: DataFrame) -> DataFrame:
        constant_columns = self.constant_columns
        key_col = self.key_col
        value_col = self.value_col

        columns_to_pivot = [i for i in dataframe.columns if i not in constant_columns]
        expression = ""
        cnt = 0
        for column in columns_to_pivot:
            cnt += 1
            if column.replace('.','',1).isdigit():
                expression += f"'{column}' , `{column}`,"
            else:
                expression += f"'{column}' , {column},"

        expression = f"stack({cnt}, {expression[:-1]}) as ({key_col}, {value_col})"
        unpivoted_df = dataframe if cnt == 0 \
            else dataframe.select(constant_columns + [expr(expression)])
        return unpivoted_df
