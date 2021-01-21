from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, to_date, to_timestamp, when, lit
import json


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        types = {i['name']: [i['type'], i['nullable']] for i in json.loads(expected_schema.json())['fields']}
        updated_df = dataframe

        for key, value in types.items():

            if value[0] == 'date':
                updated_df = updated_df.withColumn(key, to_date(col(key), 'dd-MM-yyyy'))
            elif value[0] == 'timestamp':
                updated_df = updated_df.withColumn(key, to_timestamp(col(key), 'dd-MM-yyyy HH:mm:ss'))

            updated_df = updated_df.withColumn(key, col(key).cast(value[0])) if key != 'array' else updated_df

            if value[1] == False:
                updated_df = updated_df.withColumn(key, when(col(key).isNull(), lit(False)).otherwise(col(key)))

        spark = SparkSession.builder.master('local[1]').getOrCreate()
        updated_df = spark.createDataFrame(updated_df.rdd, expected_schema)

        return updated_df
