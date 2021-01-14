from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, explode

import time


class UnpackNestedFields:
    """
    Class provides possibilities to unpack nested structures in row recursively and provide flat structure as result.
    To clarify rules, please investigate tests.
    After unpacking of structure additional columns should be provided with next name {struct_name}.{struct_field_name}

    """

    # ToDo implement unpacking of nested fields
    def unpack_nested(self, dataframe: DataFrame):

        nested = True if 'array' in ''.join([x[1] for x in dataframe.dtypes]) \
                         or 'struct' in ''.join([x[1] for x in dataframe.dtypes]) else False
        while nested:
            columns = [x for x in dataframe.columns]
            for field in dataframe.schema.fields:
                fld = field.jsonValue()

                try:
                    ftype = fld['type']
                    struct_name = fld['name']
                    if isinstance(ftype, dict):

                        if ftype['type'] == 'array':
                            dataframe = dataframe = dataframe.withColumn(struct_name, explode(struct_name))

                        if ftype['type'] == 'struct':
                            nested_cols = []
                            for el in ftype['fields']:
                                nested_cols.append((struct_name, el['name']))
                            columns.remove(struct_name)
                            dataframe = dataframe.select(
                                columns + [col(cl[0] + '.' + cl[1]).alias(cl[0] + '_' + cl[1]) for cl in nested_cols])

                except Exception:
                    raise Exception

            nested = True if 'array' in ''.join([x[1] for x in dataframe.dtypes]) \
                             or 'struct' in ''.join([x[1] for x in dataframe.dtypes]) else False

        return dataframe
