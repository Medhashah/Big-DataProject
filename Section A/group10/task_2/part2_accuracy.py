
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.types import StringType, StructType, StructField, IntegerType, DoubleType
from operator import itemgetter


spark = SparkSession \
        .builder \
        .appName("big_data_part_2_acc") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

df_label = spark.read \
    .format("csv") \
    .option("header", "true") \
    .load("/user/jz3350/big_data_labelled_files_1.csv")

df_label = df_label.filter(F.col("label").isNotNull())

df_types = spark.read.json("/user/zw1923/task2_json/*")

df_join = df_label.join(df_types, df_label.file_name == df_types.column_name)

# def cal_sum(val):
#     sum = 0
#     for i in val:
#         sum += float(i[0])
#     return sum
#
# udf_sum = F.udf(cal_sum, DoubleType())
#
# df_join = df_join.withColumn("total_count", udf_sum(df_join.semantic_types))

# def cal_accuracy(label, count, val):
#     for i in val:
#         if i[1] == label:
#             return float(i[0])/count

def pred_type(val):
    item = [0, "Other"]
    for i in val:
        if i[0] > item[0] and i[1] != "Other":
            item = i
    return item[1]


# udf_acc = F.udf(cal_accuracy, DoubleType())
#
# df_join = df_join.withColumn("acc", udf_acc(df_join.label, df_join.total_count, df_join.semantic_types))
#
# df_join = df_join.fillna({'acc':0})

udf_pred = F.udf(pred_type)

df_join = df_join.withColumn("pred", udf_pred(df_join.semantic_types))

df_label_num = df_join.groupBy("label").agg(F.count('file_name').alias('label_num'))
df_pred_num = df_join.groupBy("pred").agg(F.count('file_name').alias('pred_num'))
df_correct_num = df_join.filter(df_join.pred == df_join.label).groupBy("pred").agg(F.count('file_name').alias('correct_num')).withColumnRenamed("pred", "correct_pred")

# def precision(correct, pred):
#     return float(correct) / float(pred)

df_result = df_label_num.join(df_correct_num, df_label_num.label == df_correct_num.correct_pred, "left_outer")
df_label_correct = df_result.fillna({'correct_num':0})
df_label_correct = df_label_correct.withColumn("recall", F.col("correct_num") / F.col("label_num"))

df_pred_correct = df_pred_num.join(df_correct_num, df_pred_num.pred == df_correct_num.correct_pred, "left_outer")
df_precision = df_pred_correct.fillna({'correct_num':0})
df_precision = df_precision.withColumn("prec", F.col("correct_num") / F.col("pred_num"))
df_precision = df_precision.select("pred", "prec")
df_precision = df_precision.fillna({'prec':0})

df_final = df_label_correct.join(df_precision, df_label_correct.label == df_precision.pred, "left_outer")

labels = df_final.select("label").collect()
recalls = df_final.select("recall").collect()
precs = df_final.select("prec").collect()

f = open("plot.csv", "w")
for i in range(df_final.count()):
    f.write("{}, {}, {}\n".format(labels[i][0], recalls[i][0], precs[i][0]))
f.close()


# df_join.select("label","acc", "total_count").orderBy(F.desc("acc")).show()
# df_join.select(F.avg("acc")).show()






