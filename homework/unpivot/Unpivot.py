from typing import List

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, struct, array, explode

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
        key_columns = [f'`{x}`' for x in dataframe.columns if x not in self.constant_columns]
        if key_columns:

            # dataframe = dataframe.select(self.constant_columns)\
            #     .withColumn('key_value', lit(array(*(struct(lit(key).alias(self.key_col), col(key).alias(self.value_col)) for key in key_columns))))
            # dataframe = dataframe.select(self.constant_columns).withColumn('key_value', explode('key_value'))

            key_value = array(*(
                struct(lit(c).alias(self.key_col), col(c).alias(self.value_col))
                for c in key_columns))
            dataframe = dataframe.withColumn("key_value", explode(key_value))

            cols = self.constant_columns + [
                col("key_value")[x].alias(x) for x in [self.key_col, self.value_col]]


            dataframe = dataframe.select(*cols) \
                .sort(['id', 'name', 'date'])


        for d in dataframe.collect():
            print(d)


        return dataframe
