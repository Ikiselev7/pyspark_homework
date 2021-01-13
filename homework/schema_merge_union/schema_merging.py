from pyspark.sql import DataFrame
from pyspark.sql.functions import lit


class SchemaMerging:
    """
    Class provides possibilities to union tow datasets with different schemas.
    Result dataset should contain all rows from both with columns from both dataset.
    If columns have the same name and type - they are identical.
    If columns have different types and the same name, 2 new column should be provided with next pattern:
    {field_name}_{field_type}
    """

    def union(self, dataframe1: DataFrame, dataframe2: DataFrame):
        # check if dataframe schemas is identical
        if dataframe1.schema == dataframe2.schema:
            result_df = dataframe1.union(dataframe2)
        else:
            # check missing columns for both dataframes
            df1_missing_columns = [c for c in dataframe2.columns if c not in dataframe1.columns]
            df2_missing_columns = [c for c in dataframe1.columns if c not in dataframe2.columns]

            # check shared columns with different types
            shared_columns = [c for c in dataframe1.columns if c in dataframe2.columns]
            shared_columns = self._get_shared_columns_with_different_types(dataframe1, dataframe2, shared_columns)

            result_df = self._union_dataframes(
                dataframe1,
                dataframe2,
                df1_missing_columns,
                df2_missing_columns,
                shared_columns
            )

        return result_df

    @staticmethod
    def _add_missing_columns(dataframe, missing_col_names, cols_order_reference=None):
        missing_cols = []
    
        for col_name in missing_col_names:
            missing_cols.append(lit(None).alias(col_name))
    
        result_df = dataframe.select(*dataframe.columns, *missing_cols)
    
        # if columns order reference is provided, return in given order
        if cols_order_reference:
            return result_df.select(cols_order_reference)
        else:
            return result_df

    @staticmethod
    def _get_col_type_from_schema(dataframe, col_name):
        return list(filter(lambda x: col_name in x, dataframe.dtypes))[0][1]

    def _get_shared_columns_with_different_types(self, dataframe1, dataframe2, shared_columns):
        shared_columns_diff_type = []
    
        for col_name in shared_columns:
            if self._get_col_type_from_schema(dataframe1, col_name) != \
                    self._get_col_type_from_schema(dataframe2, col_name):
                shared_columns_diff_type.append(col_name)
    
        return shared_columns_diff_type

    def _add_shared_columns(self, dataframe, shared_col_names):
        """
        Returns dataframe with renamed columns and list of new column names
        """
        new_columns = []
    
        for col_name in shared_col_names:
            col_type = self._get_col_type_from_schema(dataframe, col_name)
            new_col_name = f'{col_name}_{col_type}'
    
            dataframe = dataframe.withColumnRenamed(col_name, new_col_name)
            new_columns.append(new_col_name)
    
        return dataframe, new_columns

    def _union_dataframes(self, dataframe1, dataframe2, df1_missing_columns, df2_missing_columns, shared_columns):
        """
        Adds missing columns to both of the dataframes and resolves conflicts in shared column types
        Returns union of provided dataframes
        """
        if shared_columns:
            dataframe1, df1_new_columns = self._add_shared_columns(dataframe1, shared_columns)
            dataframe2, df2_new_columns = self._add_shared_columns(dataframe2, shared_columns)
    
            df1_missing_columns.extend(df2_new_columns)
            df2_missing_columns.extend(df1_new_columns)
    
        dataframe1 = self._add_missing_columns(dataframe1, df1_missing_columns)
        dataframe2 = self._add_missing_columns(dataframe2, df2_missing_columns, dataframe1.columns)
    
        return dataframe1.union(dataframe2)
