from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, when



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
        pk = self.primary_keys[0]
        cols = old_dataframe.columns


        # result_dataframe = old_dataframe\
        #     .unionAll(new_dataframe.exceptAll(old_dataframe))\
        #     .dropDuplicates([pk])\
        #     .alias('resdf')\
        #     .sort([pk])

        # old_cols = old_dataframe.columns
        # new_old_cols = ['old_'+x if x != pk else x for x in old_cols]
        # old_names_tuples = zip(old_cols, new_old_cols)
        # for old_name in old_names_tuples:
        #     old_dataframe = old_dataframe.withColumnRenamed(old_name[0], old_name[1])
        #
        # new_cols = new_dataframe.columns
        # new_new_cols = ['new_' + x if x != pk else x for x in new_cols]
        # new_names_tuples = zip(new_cols, new_new_cols)
        # for new_name in new_names_tuples:
        #     new_dataframe = new_dataframe.withColumnRenamed(new_name[0], new_name[1])
        #
        # new_names = list(zip(new_old_cols, new_new_cols))
        # print(new_names)
        #
        # result_dateframe = old_dataframe\
        #     .unionAll(new_dataframe.exceptAll(old_dataframe))
        #
        # full_dataframe = old_dataframe.join(other=new_dataframe, on='id', how='full').alias('fulldf')
        #
        # for names in new_names:
        #     print(names)
        #     full_dataframe = full_dataframe.withColumn('meta',
        #                               when(col(names[0])==col(names[1]), 'not_changed')
        #                               .when(col(names[0]).isNull(), 'inserted')
        #                               .when(col(names[1]).isNull(), 'deleted')
        #                               .otherwise('changed'))
        #
        # result_dataframe = result_dataframe.join(full_dataframe, on='id', how='left').select('resdf.*', 'meta')
        #
        # resdf = result_dataframe.collect()
        # # fulldf = full_dataframe.collect()
        # for d in resdf:
        #     print(d)



        not_changed_rows = old_dataframe.join(new_dataframe, on=cols, how='semi')

        changed_rows_new = new_dataframe.join(old_dataframe, on=pk, how='semi')\
            .exceptAll(not_changed_rows)

        changed_rows_old = old_dataframe.join(new_dataframe, on=pk, how='semi') \
            .exceptAll(not_changed_rows)

        old_rows = old_dataframe\
            .exceptAll(not_changed_rows) \
            .exceptAll(changed_rows_old)

        new_rows = new_dataframe\
            .exceptAll(not_changed_rows) \
            .exceptAll(changed_rows_new)

        not_changed_rows = not_changed_rows.withColumn('meta', lit('not_changed'))
        changed_rows = changed_rows_new.withColumn('meta', lit('changed'))
        old_rows = old_rows.withColumn('meta', lit('deleted'))
        new_rows = new_rows.withColumn('meta', lit('inserted'))

        # for r in not_changed_rows.collect():
        #     print(r)
        #
        # for r in changed_rows.collect():
        #     print(r)
        #
        # for r in old_rows.collect():
        #     print(r)
        #
        # for r in new_rows.collect():
        #     print(r)

        result_dateframe = not_changed_rows\
                            .union(changed_rows)\
                            .union(old_rows)\
                            .union(new_rows)\
                            .sort('id')

        #
        resdf = result_dateframe.collect()
        for d in resdf:
            print(d)


        return result_dateframe
