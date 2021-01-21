from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, lit


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
        self.primary_keys = primary_keys

    # ToDo: Implement history product
    def get_history_product(self, old_dataframe: DataFrame, new_dataframe: DataFrame):
        old_df = old_dataframe
        new_df = new_dataframe
        pk = self.primary_keys or [i for i in old_df.columns]
        not_pk = [i for i in old_df.columns if i not in pk]

        for c in old_dataframe.columns:
            old_df = old_df.withColumn(c, when(col(c).isNull(), lit(-1)).otherwise(col(c)))
            new_df = new_df.withColumn(c, when(col(c).isNull(), lit(-1)).otherwise(col(c)))
            old_df = old_df.withColumnRenamed(c, f'{c}_old') if c not in pk else old_df

        updated_df = old_df.join(new_df, pk, 'full')
        updated_df = updated_df.withColumn('meta', lit(None))

        for c in not_pk:
            updated_df = updated_df.withColumn('meta',
                                               when(col(f'{c}_old').isNull(),
                                                    lit('inserted')).otherwise(col('meta')))
            updated_df = updated_df.withColumn('meta',
                                               when(col(c).isNull(),
                                                    lit('deleted')).otherwise(col('meta')))
            updated_df = updated_df.withColumn('meta',
                                               when(col(f'{c}_old') != col(c),
                                                    lit('changed')).otherwise(col('meta')))
            updated_df = updated_df.withColumn(c,
                                               when(col('meta') == 'deleted',
                                                    lit(col(f'{c}_old'))).otherwise(col(c)))

        if len(pk) == len(updated_df.columns) - 1:
            df_inserted_no_key = new_df.subtract(old_df)\
                .withColumn('meta', lit('inserted'))
            df_deleted_no_key = old_df.subtract(new_df)\
                .withColumn('meta', lit('deleted'))
            updated_df = df_deleted_no_key.union(df_inserted_no_key)
            df_not_changed_no_key = old_df.join(new_df, pk, 'inner')\
                .withColumn('meta', lit('not_changed'))
            updated_df = df_not_changed_no_key.union(updated_df)

        for c in updated_df.columns:
            updated_df = updated_df.withColumn(c,
                                               when(col(c) == -1,
                                                    lit(None)).otherwise(lit(col(c))))
            updated_df = updated_df.withColumn(c,
                                               when(col(c).isNull(),
                                                    lit('not_changed'))
                                               .otherwise(lit(col(c)))) if c == 'meta' else updated_df

        return updated_df.select('id', 'name', 'score', 'meta').orderBy(pk)
