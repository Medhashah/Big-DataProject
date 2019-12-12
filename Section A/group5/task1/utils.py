from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T
from pyspark.sql import functions as F
from collections import Counter


def generate_meta(spark: SparkSession, path: str):
    # read dataframe
    df = spark.read.option("delimiter", "\\t").option("header", "true").csv(path)
    file_name = path.split('/')[-1]
    cols = df.columns
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': cols
    }
    columns = []
    for col in cols:
        columns.append(profile_column_1(df, col, spark))

    metadata['columns'] = columns

    return metadata


def top_5(l):
    occurence_count = Counter(l)
    res = [r[0] for r in occurence_count.most_common(5)]

    # return [r[0] for r in occurence_count.most_common(5)]

def generate_meta1(spark: SparkSession, path: str):
    # read dataframe
    df = spark.read.option("delimiter", "\\t").option("header", "true").csv(path)
    file_name = path.split('/')[-1]
    cols = df.columns
    metadata = {
        'dataset_name': file_name,
        'key_column_candidates': cols
    }
    # df.agg(*(F.countDistinct(F.col(c)).alias(c) for c in df.columns))
    # df.agg(*(F.count(F.col(c)).alias(c) for c in df.columns))
    top_5_udf = F.udf(top_5, T.ArrayType(T.StringType()))
    # finally much faster!!
    result = df.agg(*(top_5_udf(F.collect_list(c)).alias(c) for c in df.columns)).collect()
    return metadata

def profile_column_brute_force(df: DataFrame, col: str, spark: SparkSession):
    """
    A naive version, 22 sec
    :param df:
    :param col:
    :return:
    """
    # Really slow right now!
    empty_num = df.filter(df[col].isNull()).count()
    total_num = df.count()
    freq_list = df.groupBy(col).count().orderBy('count', ascending=False)
    distinct_num = freq_list.count()
    freq = []
    for row in freq_list.take(5):
        freq.append(row[col])
    column_data = {
        'column_name': col,
        'number_non_empty_cells': total_num - empty_num,
        'number_empty_cells': empty_num,
        'number_distinct_values': distinct_num,
        'frequent_values': freq
    }
    return column_data


def profile_column(df: DataFrame, col: str, spark: SparkSession):
    """
     33 secs
    :param df:
    :param col:
    :param spark:
    :return:
    """
    # grouped_df = df.groupBy('category').agg(F.count('*').alias('freq')).orderBy('freq', ascending=False)

    grouped_df = df.groupBy(col).agg(F.count('*').alias('freq')).orderBy('freq', ascending=False)
    grouped_df.createOrReplaceTempView('group_df')
    query = f"""
    SELECT COUNT(`{col}`) as num, SUM(freq) as total, SUM(CASE WHEN `{col}` is not NULL THEN freq ELSE 0 END ) as non_empty from group_df 
    """
    result = spark.sql(query).collect()[0]

    empty_num = result.total - result.non_empty
    total_num = result.total
    non_empty_num = result.non_empty
    distinct_num = result.num
    freq = [row[col] for row in grouped_df.limit(5).collect()]
    column_data = {
        'column_name': col,
        'number_non_empty_cells': non_empty_num,
        'number_empty_cells': empty_num,
        'number_distinct_values': distinct_num,
        'frequent_values': freq
    }
    return column_data


def profile_column_1(df: DataFrame, col: str, spark: SparkSession):
    """
     33 secs
    :param df:
    :param col:
    :param spark:
    :return:
    """
    # grouped_df = df.groupBy('category').agg(F.count('*').alias('freq')).orderBy('freq', ascending=False)
    # data.agg(count(col('category')),countDistinct('category'),count(col("cetegory").isNotNull())).show()

    grouped_df = df.groupBy(col).agg(F.count('*').alias('freq')).orderBy('freq', ascending=False)
    grouped_df.filter(F.col('category').isNull())
    grouped_df.agg(F.sum('freq'))
    grouped_df.select()

    # tmp_result = spark.sql(query).collect()[0]
    #
    # empty_num = tmp_result.total - tmp_result.non_empty
    # total_num = tmp_result.total
    # non_empty_num = tmp_result.non_empty
    # distinct_num = tmp_result.num
    # freq = [row[col] for row in grouped_df.limit(5).collect()]
    # column_data = {
    #     'column_name': col,
    #     'number_non_empty_cells': non_empty_num,
    #     'number_empty_cells': empty_num,
    #     'number_distinct_values': distinct_num,
    #     'frequent_values': freq
    # }
    return {}
