from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode, col


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    # ToDo implement unpacking of nested fields
    def unpack_nested(self, dataframe: DataFrame):
        df_fields = [(field.name, field.dataType.typeName()) for field in dataframe.schema.fields]
        updated_df = dataframe
        for name, type in df_fields:
            if type == 'array':
                updated_df = dataframe.withColumn(name, explode(dataframe[name]))
                updated_df = self.unpack_nested(updated_df)
            elif type == 'struct':
                updated_df = dataframe.select(['id', 'text'] +
                                              [col(nc + '.' + c).alias(nc + '_' + c)
                                               for nc in ['struct']
                                               for c in dataframe.select(nc + '.*').columns])
                updated_df = self.unpack_nested(updated_df)
        return updated_df
