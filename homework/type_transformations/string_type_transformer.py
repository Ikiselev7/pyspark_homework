from pyspark.sql import DataFrame
from pyspark.sql.types import StructType
from pyspark.sql.functions import col, lit

from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType, TimestampType, IntegerType, \
    BooleanType, LongType, DoubleType, DecimalType, Row

class StringTypeTransformer:
    """
    Class provides transformation for columns with string types to other, where it possible.
    """

    def transform_dataframe(self, dataframe: DataFrame, expected_schema: StructType):
        pass
