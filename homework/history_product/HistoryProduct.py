from pyspark.sql import DataFrame
from pyspark.sql.functions import col, lit, expr



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

        cols = old_dataframe.columns

        try:
            pk = self.primary_keys
            expressions = []
            for p in pk:
                expressions.append(f'old.{p} <=> new.{p}')
            expression = ' and '.join(expressions)
        except TypeError:
            expressions = []
            for c in cols:
                expressions.append(f'old.{c} <=> new.{c}')
            expression = ' and '.join(expressions)

        not_changed_keys = []
        for c in cols:
            not_changed_keys.append(f'old.{c} <=> new.{c}')
        not_changed_expression = ' and '.join(not_changed_keys)



        old_dataframe = old_dataframe.alias('old')
        new_dataframe = new_dataframe.alias('new')

        not_changed_rows = old_dataframe.join(new_dataframe, expr(not_changed_expression), how='semi')

        changed_rows_new = new_dataframe.join(old_dataframe, expr(expression), how='semi')\
            .exceptAll(not_changed_rows)

        changed_rows_old = old_dataframe.join(new_dataframe, expr(expression), how='semi') \
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
        # print('=============================================')


        result_dateframe = not_changed_rows\
                            .union(changed_rows)\
                            .union(old_rows)\
                            .union(new_rows)\
                            .sort('id', 'name')

        # resdf = result_dateframe.collect()
        # for d in resdf:
        #     print(d)

        return result_dateframe
