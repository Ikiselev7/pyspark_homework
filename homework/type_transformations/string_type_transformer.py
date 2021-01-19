from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, to_date, to_timestamp, when, lit
import json

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, TimestampType, IntegerType, \
    BooleanType, LongType, DoubleType, DecimalType, Row


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
        updated_df = spark.createDataFrame(updated_df.rdd, StructType(expected_schema))

        return updated_df


if __name__ == '__main__':
    spark_session = SparkSession.builder.master('local[1]').getOrCreate()

    df = spark_session.createDataFrame(
        [
            ("1", [1, 2, 3], "19-02-2020", "19-02-2020 00:00:00", "true", "1",
             "0.5", "123534627458685341",
             "123534627458685341"),
            ("2", [1, 2, 3], "19-02-2020", "19-02-2020 00:00:00", "false", "-1",
             "0.7891412462571346431",
             "234735684679046827457",
             "234735684679046827457"),
            ("4", [1, 2, 3], "19-02-2020", "19-02-2020 00:00:00", "not_true",
             "2147483648", "42",
             "-143763583573461346.0368479672",
             "-143763583573461346.0368479672"),
            ("5", [1, 2, 3], None, None, None, None, None, None, None),
        ],
        ["id", "array", "date", "timestamp", "boolean", "integer", "double",
         "decimal(38,0)", "decimal(24,5)"]
    )

    expected_schema = StructType(
        [StructField("id", StringType()),
         StructField("array", ArrayType(LongType())),
         StructField("date", DateType()),
         StructField("timestamp", TimestampType()),
         StructField("boolean", BooleanType(), nullable=False),
         StructField("integer", IntegerType()),
         StructField("double", DoubleType()),
         StructField("decimal(38,0)", DecimalType(38, 0)),
         StructField("decimal(24,5)", DecimalType(24, 5))])

    expected_df = [
        '{"id":"1","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":true,'
        '"integer":1,"double":0.5,"decimal(38,0)":123534627458685341,"decimal(24,5)":123534627458685341.00000}',
        '{"id":"2","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":false,'
        '"integer":-1,"double":0.7891412462571347,"decimal(38,0)":234735684679046827457}',
        '{"id":"4","array":[1,2,3],"date":"2020-02-19","timestamp":"2020-02-19T00:00:00.000+03:00","boolean":false,'
        '"double":42.0,"decimal(38,0)":-143763583573461346,"decimal(24,5)":-143763583573461346.03685}',
        '{"id":"5","array":[1,2,3],"boolean":false}']

    transformer = StringTypeTransformer()
    transformed = transformer.transform_dataframe(df, expected_schema)
    transformed.printSchema()
    transformed.show()
    print(transformed.schema == expected_schema)
    print(transformed.toJSON().collect() == expected_df)
