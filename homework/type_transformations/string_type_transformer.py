from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit, when, udf, regexp_replace
from pyspark.sql import SparkSession, SQLContext

from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, TimestampType, IntegerType, \
    BooleanType, LongType, DoubleType, DecimalType, Row


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):

        # dataframe = dataframe.na.fill({'boolean':False})
        for e in expected_schema:
            print(e)
        print('_______________')

        for column in expected_schema:
            # print(column.jsonValue())
            name = column.name
            etype = column.dataType
            nullable = column.nullable
            print(etype)

            if name=='boolean':
                dataframe = dataframe.withColumn(name, regexp_replace(name, 'not_true', 'False'))
                dataframe.schema[name].nullable = column.nullable
                dataframe.schema[name].dataType = etype
                dataframe = dataframe.withColumn(name, when(col(name).isNull(), False).otherwise(col(name)==True).cast(etype))
            elif name != 'array':
                dataframe = dataframe.withColumn(name, when(col(name).isNotNull(), col(name)).otherwise(lit(None)).cast(etype))
            #     dataframe = dataframe.withColumn(name, regexp_replace(name, '_', ''))




            print(dataframe.schema[name])

                # dataframe = dataframe.withColumn(name, col(name).alias(column))

                # dataframe = dataframe.withColumn(name, udf_foo(name))

        for f in dataframe.schema:
            print(f)

        print('----------------------------')

        for f in dataframe.collect():
            print(f)

        print(dataframe.schema)

        return dataframe

    # ("1", [1, 2, 3], "19-02-2020", "19-02-2020 00:00:00", "true", "1", "0.5", "123534627458685341",
    #  "123534627458685341")

    # ["id", "array", "date", "timestamp", "boolean", "integer", "double", "decimal(38,0)", "decimal(24,5)"]