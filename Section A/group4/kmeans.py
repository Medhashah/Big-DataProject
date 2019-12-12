import sys
import pyspark
import string
import re
from pyspark.ml.feature import Word2Vec


import pyspark
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.ml.feature import RegexTokenizer, StringIndexer

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, LongType, BooleanType,ArrayType
from pyspark.ml.clustering import KMeans

sc = SparkContext()
spark = SparkSession.builder.appName("task2").config("spark.some.config.option", "some-value").getOrCreate()
sqlContext = SQLContext(spark)
resub = F.udf(lambda string: re.sub(r'[^\w\s]','',string), StringType())

#collect data to rdd
cluster = sc.textFile("cluster2.txt").collect()
inp = sc.emptyRDD()
for file in cluster:
	filePath = "/user/hm74/NYCColumns/" + file.replace("'","")
	tmp = sc.textFile(filePath).map(lambda row: row.split("\t")).map(lambda x:(str(x[0]),x[1]))
	inp = sc.union([inp, tmp])

inp = inp.reduceByKey(lambda x,y: int(x)+int(y))

df = sqlContext.createDataFrame(inp, ['inp', 'count'])
df = df.withColumn("sentence", resub(df.inp))

#tokenized words
regexTokenized = RegexTokenizer(inputCol="sentence", outputCol="words").transform(df)
regexTokenized = regexTokenized.select("sentence", "words", "count")

#transform word to vec
print("word2vec")
word2vec = Word2Vec(inputCol="words", outputCol="features").setMinCount(0)
model = word2vec.fit(regexTokenized)
result = model.transform(regexTokenized)
result.createOrReplaceTempView("result")

#dulicate dataset to fit kmeans
print("flat")
n_to_array = F.udf(lambda n : [1] * int(n), ArrayType(IntegerType()))
df2 = result.withColumn("n", n_to_array(result["count"]))
flat = df2.withColumn('n', F.explode(df2.n))

#fit kmeans and save the model
print("kmeans")
kmeans = KMeans().setK(23).setSeed(1)
model = kmeans.fit(flat)
model.save("flat_kmeans")
wssse = model.computeCost(flat)
print("cost = ",wssse)
centers = model.clusterCenters()
print(centers)

output = model.transform(flat)
output.createOrReplaceTempView("output")
output = output.distinct()
print("saving output")
output.write.save("kmeans.parquet", format="parquet")
for i in range(23):
	output.select("sentence", "prediction").where("prediction == {0}".format(i)).show()

