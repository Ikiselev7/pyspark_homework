import pyspark


# import sys
# sys.path.append('../..')

from homework.deduplicate.deduplicate import Deduplicator


def test_deduplicate_no_keys(spark_session: pyspark.sql.SparkSession):
    df = spark_session.createDataFrame(
        [(1, "Account_1", 30.5), (1, "Account_1", 30.6), (1, "Account_2", 30.6), (1, "Account_1", 30.5)],
        ['id', 'account', 'score']
    )

    deduplicator = Deduplicator()
    actual_df = deduplicator.deduplicate([], df).collect()
    assert actual_df == [(1, "Account_1", 30.5), (1, "Account_1", 30.6), (1, "Account_2", 30.6)]


def test_deduplicate_by_id(spark_session: pyspark.sql.SparkSession):
    df = spark_session.createDataFrame(
        [(1, "Account_1", 30.5), (1, "Account_1", 30.6), (1, "Account_2", 30.6), (1, "Account_1", 30.5)],
        ['id', 'account', 'score']
    )

    deduplicator = Deduplicator()
    actual_df = deduplicator.deduplicate('id', df).collect()
    assert actual_df == [(1, "Account_1", 30.5)]


def test_deduplicate_by_id_and_account(spark_session: pyspark.sql.SparkSession):
    df = spark_session.createDataFrame(
        [(1, "Account_1", 30.5), (1, "Account_1", 30.6), (1, "Account_2", 30.6), (1, "Account_1", 30.5)],
        ['id', 'account', 'score']
    )

    deduplicator = Deduplicator()
    actual_df = deduplicator.deduplicate(['id', 'account'], df).collect()
    assert actual_df == [(1, "Account_1", 30.5), (1, "Account_2", 30.6)]



# def main():
#     spark = pyspark.sql.SparkSession.builder \
#         .master('local[1]') \
#         .getOrCreate()
#     test_deduplicate_no_keys(spark)
#     test_deduplicate_by_id(spark)
#     test_deduplicate_by_id_and_account(spark)
#
#
# if __name__=='__main__':
#     main()