from pyspark.sql import SparkSession
import sys 
from pyspark.sql.functions import *
from pyspark.sql.functions import format_string
from pyspark.sql.functions import year, month, dayofmonth

spark = SparkSession.builder.appName("Python Spark SQL basic example").config("spark.some.config.option", "some-value").getOrCreate()



parking = spark.read.format("com.databricks.spark.csv").option("header", "true").option("delimiter", ",").option("timestampFormat", "MM/dd/yyyy hh:mm:ss").option("inferSchema", "true").option("delimiter","\t").option("nullValue", "null").load(sys.argv[1])
# dataset =spark.read.format('csv').options(header='true',inferschema='true',delimiter='\t').load(sys.argv[1])dataset =spark.read.format('csv').options(header='true',inferschema='true',delimiter='\t').load(sys.argv[1])
parking.createOrReplaceTempView("parking")
res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from parking where Borough = 'BROOKLYN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Brooklyn_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from parking where Borough = 'MANHATTAN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Manhattan_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from parking where Borough = 'QUEENS' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Queens_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from parking where Borough = 'STATEN ISLAND' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("StatenIsland_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from parking where Borough = 'BRONX' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Bronx_Complaints.out",format="text")

spark.sql("select count(*) from parking").show()
split = spark.sql("select `Complaint Type`, `Created Date`, Borough, year(`Created Date`) as year, month(`Created Date`) as month from parking order by year DESC")
split.createOrReplaceTempView("split")
#split.show(40)
#split.printSchema()
fall = spark.sql("select * from split where month == 09 or month == 10 or month == 11")
fall.createOrReplaceTempView("fall")
winter = spark.sql("select * from split where month == 12 or month == 01 or month == 02")
winter.createOrReplaceTempView("winter")
spring = spark.sql("select * from split where month == 03 or month == 04 or month == 05")
spring.createOrReplaceTempView("spring")
summer = spark.sql("select * from split where month == 06 or month == 07 or month == 08")
summer.createOrReplaceTempView("summer")


#during fall
print("During Fall in All Boroughs")
res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from fall where Borough = 'BROOKLYN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Brooklyn_Fall_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from fall where Borough = 'MANHATTAN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Manhattan_Fall_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from fall where Borough = 'QUEENS' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Queens_Fall_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from fall where Borough = 'STATEN ISLAND' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Staten_Island_Fall_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from fall where Borough = 'BRONX' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Bronx_Fall_Complaints.out",format="text")



print("During winter")
spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from winter where Borough = 'BROOKLYN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Brooklyn_Winter_Complaints.out",format="text")

spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from winter where Borough = 'MANHATTAN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Manhattan_Winter_Complaints.out",format="text")

spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from winter where Borough = 'QUEENS' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Queens_Winter_Complaints.out",format="text")

spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from winter where Borough = 'STATEN ISLAND' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("StatenIsland_Winter_Complaints.out",format="text")

spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from winter where Borough = 'BRONX' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Bronx_Winter_Complaints.out",format="text")


print("During Spring")
res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from spring where Borough = 'BROOKLYN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Brooklyn_Spring_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from spring where Borough = 'MANHATTAN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Manhattan_Spring_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from spring where Borough = 'QUEENS' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Queens_Spring_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from spring where Borough = 'STATEN ISLAND' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("StatenIsland_Spring_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from spring where Borough = 'BRONX' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Bronx_Spring_Complaints.out",format="text")




print("During Summer")
res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from summer where Borough = 'BROOKLYN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Brooklyn_Summer_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from summer where Borough = 'MANHATTAN' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Manhattan_Summer_Complaints.out",format="text")

res=spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from summer where Borough = 'QUEENS' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Queens_Summer_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from summer where Borough = 'STATEN ISLAND' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("StatenIsland_Summer_Complaints.out",format="text")

res = spark.sql("select `Complaint Type` as ctype, count(`Complaint Type`) as ct from summer where Borough = 'BRONX' group by ctype order by ct desc")
res.select(format_string('%s, %d',res.ctype, res.ct)).write.save("Bronx_Summer_Complaints.out",format="text")



#spark.sql("select * from split where year == 2006").show()
#spark.sql("select * from parking where year(`Created Date`) == 2019").show()

