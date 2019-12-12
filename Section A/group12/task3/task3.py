import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, format_string
from pyspark.sql.types import StringType
from pyspark.sql.types import DoubleType
from pyspark.sql.types import DateType
from pyspark.sql.types import IntegerType
from pyspark.sql import functions as F
from pyspark.sql import Row

spark = SparkSession \
 .builder \
 .appName("Python Spark SQL basic example") \
 .config("spark.some.config.option", "some-value") \
 .getOrCreate()


import numpy as np
import pandas as pd


df = spark.read.format('csv').options(header='true',inferschema='true').option("delimiter", "\t").load("/user/hm74/NYCOpenData/hy4q-igkk.tsv.gz")
df.createOrReplaceTempView("df")
#query = "select column_name from information_schema where table_name='2232-dj5q'"
#df = spark.sql(query)
print(df.schema.names)

def correct_zip(zip_code):
    try:
        zip_code = int(float(zip_code))
    except:
        try:
            zip_code = int(float(zip_code.split('-')[0]))
        except:
            return None
    if zip_code < 10000 or zip_code > 19999:
        return None
    else:
        return str(zip_code)

def remove_borough(b):
    if b=="Unspecified":
        return None
    else:
        return b

def create_date(date):
    datel = date.split(" ")
    return datel[0]

def create_month(date):
    datel = date.split("/")
    try:
        return int(datel[0])
    except:
        return 

create_month = F.udf(create_month, IntegerType())
create_date = F.udf(create_date, StringType())
remove_borough = F.udf(remove_borough, StringType())
correct_zip = F.udf(correct_zip, StringType())

df = df.withColumn('borough', remove_borough(F.col("Borough")))
df = df.withColumn('created_date', create_date(F.col("Created Date")))
df = df.withColumn('closed_date', create_date(F.col("Closed Date")))
df = df.withColumn('created_month', create_month(F.col("Created Date")))
df = df.filter(df.borough.isNotNull())
df = df.filter(df.Latitude.isNotNull())
df = df.filter(df.Longitude.isNotNull())
df = df.filter(df.created_month.isNotNull())
df = df.filter(F.col("Closed Date").isNotNull())
#df.select('created_month', 'closed_date').show()


df.createOrReplaceTempView("df")
bronx = spark.sql("select `Complaint Type`, Borough, count(*) from df where Borough=\"BRONX\" group by Borough, `Complaint Type` order by Borough, count(*) desc limit 3")
manhattan = spark.sql("select `Complaint Type`, Borough, count(*) from df where Borough=\"MANHATTAN\" group by Borough, `Complaint Type` order by Borough, count(*) desc limit 3")
staten_island = spark.sql("select `Complaint Type`, Borough, count(*) from df where Borough=\"STATEN ISLAND\" group by Borough, `Complaint Type` order by Borough, count(*) desc limit 3")
brooklyn = spark.sql("select `Complaint Type`, Borough, count(*) from df where Borough=\"BROOKLYN\" group by Borough, `Complaint Type` order by Borough, count(*) desc limit 3")
queens = spark.sql("select `Complaint Type`, Borough, count(*) from df where Borough=\"QUEENS\" group by Borough, `Complaint Type` order by Borough, count(*) desc limit 3")
# df.groupBy("Complaint Type", "Borough").count().select('Complaint Type','Borough', F.col("count").alias("ctr")).orderBy([F.col("ctr"), F.col("Borough")], ascending=False).show(df.count(), truncate=False)

bronx.show(truncate=False)
manhattan.show(truncate=False)
staten_island.show(truncate=False)
brooklyn.show(truncate=False)
queens.show(truncate=False)


newdf = df.select("created_date", "closed_date", "created_month", "Complaint Type", "Borough", "Agency")
newdf = newdf.select(F.col("*"), F.to_date("created_date", "MM/dd/yyyy").alias("unix_created"))
newdf = newdf.select(F.col("*"), F.to_date("closed_date", "MM/dd/yyyy").alias("unix_closed"))
newdf.select("created_month","created_date", "unix_created", "closed_date", "unix_closed").show()
# datedf.show(truncate=False)
newdf.filter(newdf.unix_closed >= newdf.unix_created).show()



timestamp = (F.unix_timestamp('unix_closed', "yyyy-MM-dd") - F.unix_timestamp('unix_created', "yyyy-MM-dd"))/86400
newdf = newdf.withColumn("process_time", timestamp)
newdf.filter(newdf.Borough=="MANHATTAN").select("process_time").show()


newdf.groupBy("Borough").agg({'process_time':'avg', 'borough':'count'}).show()
newdf.groupBy("Agency").agg({'process_time':'avg', 'borough':'count'}).show()

newdf.createOrReplaceTempView("newdf")
print("Change over the months for Brooklyn")
print("For Winter")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=12 or created_month=1 or created_month=2 and Borough=\"BROOKLYN\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Spring")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=3 or created_month=4 or created_month=5 and Borough=\"BROOKLYN\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Summer")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=6 or created_month=7 or created_month=8 and Borough=\"BROOKLYN\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Fall")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=9 or created_month=10 or created_month=11 and Borough=\"BROOKLYN\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)

print("Change over the months for QUEENS")
print("For Winter")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=12 or created_month=1 or created_month=2 and Borough=\"QUEENS\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Spring")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=3 or created_month=4 or created_month=5 and Borough=\"QUEENS\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Summer")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=6 or created_month=7 or created_month=8 and Borough=\"QUEENS\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)
print("For Fall")
spark.sql("select `Complaint Type`, count(*) as ctr from newdf where created_month=9 or created_month=10 or created_month=11 and Borough=\"QUEENS\" group by `Complaint Type` order by ctr desc limit 5").show(truncate=False)



