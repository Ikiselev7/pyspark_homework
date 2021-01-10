from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when, array, array_distinct


class HistoryProduct:
    """
    Class provides possibilities to compute history of rows.
    You should compare old and new dataset and define next for each row:
     - row was changed
     - row was inserted
     - row was deleted
     - row not changed
     Result should contain all rows from both datasets and new column `meta`
     where status of each row should be provided ('not_changed', 'changed', 'inserted', 'deleted')
    """
    def __init__(self, primary_keys=None):
        if primary_keys:
            self.primary_keys = primary_keys
        else:
            self.primary_keys = []

    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        non_primary_key_column_names = [c for c in old_dataframe.columns if c not in self.primary_keys]

        # defining clauses for 'meta' column
        meta_column = when(col('o.exists').isNull(), 'inserted') \
            .when(col('n.exists').isNull(), 'deleted')

        for c in non_primary_key_column_names:
            meta_column = meta_column.when(~old_dataframe[c].eqNullSafe(new_dataframe[c]), 'changed')

        meta_column = meta_column.otherwise('not_changed')

        # join old and new dataframes and adding 'meta' column
        on = [col('o.' + c).eqNullSafe(col('n.' + c)) for c in self.primary_keys] or None
        result_df = old_dataframe.withColumn('exists', lit(1)).alias('o') \
            .join(new_dataframe.withColumn('exists', lit(1)).alias('n'), on, 'fullouter') \
            .withColumn('meta', meta_column)

        # suffix for result column names
        col_suffix = '_res'

        # adding result columns to result_df
        for c in non_primary_key_column_names:
            result_df = result_df.withColumn(c + col_suffix,
                                             when(col('n.exists') == 1, col('n.' + c)).otherwise(col('o.' + c)))

        # selecting only result columns from result_df
        primary_key_cols = [when(col('n.' + c).isNotNull(), col('n.' + c)).otherwise(col('o.' + c))
                            for c in self.primary_keys]
        non_primary_key_cols = [col(c + col_suffix) for c in non_primary_key_column_names]

        result_df = result_df.select(*primary_key_cols, *non_primary_key_cols, col('meta'))

        # renaming result columns
        non_primary_key_column_renamed = [c[:-len(col_suffix)] for c in result_df.columns if c.endswith(col_suffix)]

        temp_names = [c for c in result_df.columns if c not in self.primary_keys]

        for old_name, new_name in zip(temp_names, non_primary_key_column_renamed):
            result_df = result_df.withColumnRenamed(old_name, new_name)

        return result_df
