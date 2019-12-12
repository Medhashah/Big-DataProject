from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

sc = SparkContext('local')
spark = SparkSession(sc)

data = spark.read.format('csv').options(sep='\t',header='true',inferschema='true').load("/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz")

data.createOrReplaceTempView("Complaints")

# 3 most Frequent Complaints according to Borough

sqlDF = spark.sql("Select 'Complaint Type',count(*) from Complaints where Borough='MANHATTAN' group by 'Complaint Type' order by count(*) desc limit 3")
sqlDF.show()
sqlDF.write.csv("Manhattan.csv")

sqlDF = spark.sql("Select 'Complaint Type',count(*) from Complaints where Borough='BROOKLYN' group by 'Complaint Type' order by count(*) desc limit 3")
sqlDF.show()
sqlDF.write.csv("Brooklyn1.csv")

sqlDF = spark.sql("Select 'Complaint Type',count(*) from Complaints where Borough='BRONX' group by 'Complaint Type' order by count(*) desc limit 3")
sqlDF.show()
sqlDF.write.csv("BRONX1.csv")

sqlDF = spark.sql("Select 'Complaint Type',count(*) from Complaints where Borough='QUEENS' group by 'Complaint Type' order by count(*) desc limit 3")
sqlDF.show()
sqlDF.write.csv("QUEENS1.csv")

sqlDF = spark.sql("Select 'Complaint Type',count(*) from Complaints where Borough='STATEN ISLAND' group by 'Complaint Type' order by count(*) desc limit 3")
sqlDF.show()
sqlDF.write.csv("SI1.csv")


# Most Recorded Complaints by Agencies in every Borough 

sqlDF = spark.sql("Select 'Agency Name',count(*) from Complaints where Borough='MANHATTAN' group by 'Agency Name' order by count(*) desc limit 1")
sqlDF.show()
sqlDF.write.csv("Manhattan1.csv")

sqlDF = spark.sql("Select 'Agency Name',count(*) from Complaints where Borough='BROOKLYN' group by 'Agency Name' order by count(*) desc limit 1")
sqlDF.show()
sqlDF.write.csv("Brooklyn1.csv")

sqlDF = spark.sql("Select 'Agency Name',count(*) from Complaints where Borough='STATEN ISLAND' group by 'Agency Name' order by count(*) desc limit 1")
sqlDF.show()
sqlDF.write.csv("SI1.csv")

sqlDF = spark.sql("Select 'Agency Name',count(*) from Complaints where Borough='BRONX' group by 'Agency Name' order by count(*) desc limit 1")
sqlDF.show()
sqlDF.write.csv("BRONX1.csv")

sqlDF = spark.sql("Select 'Agency Name',count(*) from Complaints where Borough='QUEENS' group by 'Agency Name' order by count(*) desc limit 1")
sqlDF.show()
sqlDF.write.csv("QUEENS1.csv")

 
sqlDF=spark.sql("SELECT distinct(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))) AS newdate,count(*) from Complaints group by newdate ")
sqlDF.coalesce(1).write.csv("Q6total.csv",header='true')
sqlDF.show()

#Average duration that each agency has closed a service request in
sqlDF=spark.sql("SELECT Avg(datediff(TO_DATE(CAST(UNIX_TIMESTAMP(`Closed Date`, 'MM/dd/yyyy') AS TIMESTAMP)),TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP)))) as `Avg Duration`,`Agency Name` from Complaints group by `Agency Name`")
sqlDF.coalesce(1).write.csv("Q5.csv",header='true')

#Complaints change over time 
sqlDF=spark.sql("select TO_DATE(CAST(UNIX_TIMESTAMP(`Closed Date`, 'MM/dd/yyyy') AS TIMESTAMP)) as newdate,`Complaint Type`,Borough from Complaints")
sqlDF.coalesce(1).write.csv("Q3bd.csv",header='true')

#Number of Requests submitted by Mobile 
sqlDF=spark.sql("SELECT distinct(TO_DATE(CAST(UNIX_TIMESTAMP(`Created Date`, 'MM/dd/yyyy') AS TIMESTAMP))) AS newdate,count(*) from Complaints where `Open Data Channel Type`='MOBILE' group by newdate ") 
sqlDF.coalesce(1).write.csv("Q6.csv",header='true')
















