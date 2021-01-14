from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import explode


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    def unpack_nested(self, dataframe: DataFrame):
        nested_fields = self._get_nested_fields(dataframe)

        for col_name, col_type in nested_fields:
            if col_type[:5] == 'array':
                dataframe = dataframe.withColumn(col_name, explode(col_name))
            else:
                old_cols_to_select = [c for c in dataframe.columns if c != col_name]
                field_names = self._get_struct_field_names(dataframe, col_name)
                new_cols = [dataframe[col_name][field_name].alias(f'{col_name}_{field_name}') for field_name in field_names]
                dataframe = dataframe.select(*old_cols_to_select, *new_cols)

            dataframe = self.unpack_nested(dataframe)

        return dataframe

    @staticmethod
    def _get_nested_fields(df):
        return list(filter(lambda x: x[1][:5] == 'array' or x[1][:6] == 'struct', df.dtypes))

    @staticmethod
    def _get_struct_field_names(df, col_name):
        return list(filter(lambda x: x.name == col_name, df.schema.fields))[0].dataType.fieldNames()
