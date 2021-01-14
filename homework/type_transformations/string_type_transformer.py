from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, to_date, to_timestamp, when
from pyspark.sql.types import StructType, DateType, TimestampType, BooleanType


class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        # transforming dataframe according to expected schema
        for f in expected_schema.fields:
            if isinstance(f.dataType, DateType):
                dataframe = dataframe.withColumn(f.name, to_date(col(f.name), 'd-M-y'))
            elif isinstance(f.dataType, TimestampType):
                dataframe = dataframe.withColumn(f.name, to_timestamp(col(f.name), 'd-M-y H:m:s'))
            elif isinstance(f.dataType, BooleanType):
                dataframe = dataframe.withColumn(f.name, when(
                    col(f.name).isin(['true', 'True', '1', 'y', 'Y', 'yes', 'Yes']),
                    lit('true').cast(BooleanType())
                ).otherwise(lit('false').cast(BooleanType())))
                dataframe.schema[f.name].nullable = False
            else:
                dataframe = dataframe.withColumn(f.name, col(f.name).cast(f.dataType))

        # updating schema
        for c in dataframe.schema:
            if isinstance(c.dataType, BooleanType):
                c.nullable = False

        return dataframe
