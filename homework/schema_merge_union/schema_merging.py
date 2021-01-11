from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit

class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    # ToDo: Implement dataset union with schema merging
    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):


        print(dataframe2.dtypes)

        columns = {}

        columns = {col[0]: {'df1': col[1]} for col in dataframe1.dtypes}
        for col in dataframe2.dtypes:
            try:
                columns[col[0]]['df2'] = col[1]
            except KeyError:
                columns[col[0]] = {'df2': col[1]}

        print(columns)

        for col in columns:
            if 'df1' in columns[col].keys() and 'df2' in columns[col].keys():
                type1 = columns[col]['df1']
                type2 = columns[col]['df2']
                if type1 != type2:

                    if type1=='string':
                        print('here23')
                        dataframe1 = dataframe1\
                            .withColumn(f'{col}_{type1}', lit(None).cast(columns[col]['df1']))\
                            .withColumn(f'{col}_{type2}', dataframe1[col].cast(type2))\
                            .drop(col)
                        dataframe2 = dataframe2 \
                            .withColumnRenamed(col, f'{col}_{type2}') \
                            .withColumn(f'{col}_{type1}', lit(None).cast(columns[col]['df1']))\
                            .drop(col)

                    elif type2=='string':
                        print('here34')
                        dataframe2 = dataframe2 \
                            .withColumn(f'{col}_{type2}', lit(None).cast(columns[col]['df2'])) \
                            .withColumn(f'{col}_{type1}', dataframe2[col].cast(type1))\
                            .drop(col)

                        dataframe1 = dataframe1\
                            .withColumnRenamed(col, f'{col}_{type1}') \
                            .withColumn(f'{col}_{type2}', lit(None).cast(columns[col]['df2'])) \
                            .drop(col)

            elif 'df1' not in columns[col].keys():
                dataframe1 = dataframe1.withColumn(col, lit(None).cast(columns[col]['df2']))

            elif 'df2' not in columns[col].keys():

                dataframe2 = dataframe2.withColumn(col, lit(None).cast(columns[col]['df1']))

        dataframe1.printSchema()
        dataframe2.printSchema()

        res = dataframe1.unionByName(dataframe2)
        res.printSchema()
        for d in res.collect():
            print(d)
        return res