import pyspark
from pyspark.sql import SparkSession
import pytest

import sys
sys.path.append('../..')
from homework.illegal_char.illegal_char_remover import IllegalCharRemover



def test_simple(spark_session: pyspark.sql.SparkSession):
    df_1 = spark_session.createDataFrame([(1, 'qwerty123'), (2, 'asdfgh123')], ['id', 'string'])

    remover = IllegalCharRemover(['1', '2', '3'], '')

    df_2 = remover.remove_illegal_chars(df_1, 'string', 'string_filtered')

    fields = [field.name for field in df_2.schema.fields]

    assert fields == ['id', 'string_filtered']
    assert df_2.collect() == [(1, 'qwerty'), (2, 'asdfgh')]


def test_remove_special(spark_session: pyspark.sql.SparkSession):
    df_1 = spark_session.createDataFrame([(1, 'qwerty{\\/[]}^'), (2, 'asdfgh')], ['id', 'string'])

    remover = IllegalCharRemover(['^', '\\', '/', '[', ']', '{', '}'], '')

    df_2 = remover.remove_illegal_chars(df_1, 'string', 'string_filtered')

    fields = [field.name for field in df_2.schema.fields]

    assert fields == ['id', 'string_filtered']
    assert df_2.collect() == [(1, 'qwerty'), (2, 'asdfgh')]


def test_remove_empty_should_fail(spark_session: pyspark.sql.SparkSession):
    with pytest.raises(ValueError):
        IllegalCharRemover([], '')


def test_remove_none_should_fail(spark_session: pyspark.sql.SparkSession):
    with pytest.raises(ValueError):
        IllegalCharRemover(None, '')


def test_replace_none_should_fail(spark_session: pyspark.sql.SparkSession):
    with pytest.raises(ValueError):
        IllegalCharRemover(['^', '\\', '/', '[', ']', '{', '}'], None)


def main():
    spark = SparkSession.builder \
        .master('local[1]') \
        .getOrCreate()

    print('test1')
    test_simple(spark)
    print('test2')
    test_remove_special(spark)
    print('test3')
    test_remove_empty_should_fail(spark)
    print('test4')
    test_remove_none_should_fail(spark)
    print('test5')
    test_replace_none_should_fail(spark)



if __name__ == '__main__':
    main()
