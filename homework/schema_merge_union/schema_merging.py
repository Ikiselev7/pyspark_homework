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
        df_1 = dataframe1
        df_2 = dataframe2

        for col in df_2.columns:
            df_1 = df_1.withColumn(col, lit(None)) if col not in df_1.columns else df_1

        for col in df_1.columns:
            df_2 = df_2.withColumn(col, lit(None)) if col not in df_2.columns else df_2

        columns_to_join = df_1.columns
        df_1.printSchema()
        df_2.printSchema()
        types_df_1 = {key: value for key, value in df_1.dtypes}
        types_df_2 = {key: value for key, value in df_2.dtypes}

        for col in df_1.columns:
            if types_df_1[col] != types_df_2[col] \
                    and types_df_1[col] != 'null' \
                    and types_df_2[col] != 'null':
                df_1 = df_1.withColumn(f'{col}_{types_df_1[col]}', df_1[col]).drop(col)
                df_2 = df_2.withColumn(f'{col}_{types_df_2[col]}', df_2[col]).drop(col)
                columns_to_join.remove(col)

        new_df = df_1.join(df_2, columns_to_join, 'full')
        new_df = new_df.orderBy([new_df[col].asc_nulls_last() for col in df_1.columns])
        return new_df
