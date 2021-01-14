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
        for column in expected_schema:
            name = column.name
            etype = column.dataType
            nullable = column.nullable

            if name == 'boolean':
                dataframe = dataframe.withColumn(name, regexp_replace(name, 'not_true', 'False'))
                dataframe.schema[name].nullable = column.nullable
                dataframe.schema[name].dataType = etype
                dataframe = dataframe.withColumn(name, when(col(name).isNull(), False)
                                                 .otherwise(col(name) == True).cast(etype))
            elif name != 'array':
                dataframe = dataframe.withColumn(name, when(col(name).isNotNull(), col(name))
                                                 .otherwise(lit(None)).cast(etype))

        return dataframe
