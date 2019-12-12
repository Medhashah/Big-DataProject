import time
import sys
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark import SparkContext

import plotly.offline as py
import plotly.graph_objs as go

spark = SparkSession.builder.master("local").appName("DataCleaning311").getOrCreate()
data = spark.read.csv(path='/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz', sep='\t', header=True, inferSchema=False)
#data.count()
colData = []
length = data.count()
for columns in data.columns:
	colData.append(data.select([count(when((col(columns)=="NA") | (col(columns)=="Unspecified") | (col(columns)=="N/A") | (col(columns)=="") | (col(columns).isNull()) | (col(columns)=="0 Unspecified"),columns)).alias(columns)]).take(1)[0][0])

for i in range(0,len(colData)):
	colData[i]=(colData[i]/length)*100

headers=data.columns
for i in range(0,len(colData)):
	if(colData[i]>60):
		data=data.drop(headers[i])

data=data.withColumn("Closed Date",to_timestamp(col("Closed Date"),"M/d/y h:m:s a"))
data=data.withColumn("Created Date",to_timestamp(col("Created Date"),"M/d/y h:m:s a"))
data=data.withColumn("Resolution Action Updated Date",to_timestamp(col("Resolution Action Updated Date"),"M/d/y h:m:s a"))


data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Street.*","Street Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Highway.*","Highway Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Noise.*","Noise Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Taxi.*","Taxi Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Water.*","Water Complaint"))
data=data.withColumn("Complaint Type", regexp_replace(data["Complaint Type"],"Ferry.*","Ferry Complaint"))

data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10451) & (col("Incident Zip")<=10475),"BRONX").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11201) & (col("Incident Zip")<=11239),"BROOKLYN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10001) & (col("Incident Zip")<=10280),"MANHATTAN").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=10301) & (col("Incident Zip")<=10314),"STATEN ISLAND").otherwise(col("Borough")))
data=data.withColumn("Borough", when((col("Borough").isNull()) & (col("Incident Zip")>=11354) & (col("Incident Zip")<=11697),"QUEENS").otherwise(col("Borough")))

data = data.drop('Park Facility Name')
#data.toPandas().to_csv("clean.csv",index=False)
#data.coalesce(1).write.csv("Cleaned_311",header=True)
data.write.csv("hdfs://dumbo/user/sl5202/clean.csv",header=True)

data=spark.read.csv('clean.csv',header=True)
data.createOrReplaceTempView("table")

#top 3 complaints per borough
print("Brooklyn")
spark.sql('SELECT `Complaint Type`, COUNT(*) as Count FROM table WHERE Borough="BROOKLYN" GROUP BY `BOROUGH`,`Complaint Type` ORDER BY Count DESC LIMIT 3').show()
print("Manhattan")
spark.sql('SELECT `Complaint Type`, COUNT(*) as Count FROM table WHERE Borough="MANHATTAN" GROUP BY `BOROUGH`,`Complaint Type` ORDER BY Count DESC LIMIT 3').show()
print("Queens")
spark.sql('SELECT `Complaint Type`, COUNT(*) as Count FROM table WHERE Borough="QUEENS" GROUP BY `BOROUGH`,`Complaint Type` ORDER BY Count DESC LIMIT 3').show()
print("Bronx")
spark.sql('SELECT `Complaint Type`, COUNT(*) as Count FROM table WHERE Borough="BRONX" GROUP BY `BOROUGH`,`Complaint Type` ORDER BY Count DESC LIMIT 3').show()
print("Staten Island")
spark.sql('SELECT `Complaint Type`, COUNT(*) as Count FROM table WHERE Borough="STATEN ISLAND" GROUP BY `BOROUGH`,`Complaint Type` ORDER BY Count DESC LIMIT 3').show()

##Complaints Over the Years
countbrooklyn=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="BROOKLYN" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
plote = go.Scatter(x=countbrooklyn.toPandas()['Year'],y=countbrooklyn.toPandas()['COUNTS'],name='Brooklyn')
py.plot({'data':[plote],'layout':{'title':'Number of Complaints in Brooklyn'}},	filename='Brooklyn_Count')
countqueens=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="QUEENS" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
plote = go.Scatter(x=countqueens.toPandas()['Year'],y=countqueens.toPandas()['COUNTS'],name='Queen')
py.plot({'data':[plote],'layout':{'title':'Number of Complaints in Queens'}},filename='Queens_Count')
countmanhattan=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="MANHATTAN" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
plote = go.Scatter(x=countmanhattan.toPandas()['Year'],y=countmanhattan.toPandas()['COUNTS'],name='Manhattan')
py.plot({'data':[plote],'layout':{'title':'Number of Complaints in Manhattan'}},filename='Manhattan_Count')
countbronx=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="BRONX" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
plote = go.Scatter(x=countbronx.toPandas()['Year'],y=countbronx.toPandas()['COUNTS'],name='Bronx')
py.plot({'data':[plote],'layout':{'title':'Number of Complaints in Bronx'}},filename='Bronx_Count')
countisland=spark.sql('SELECT COUNT(*) AS COUNTS, SUBSTRING(`CREATED DATE`,0,4) AS Year FROM TABLE WHERE BOROUGH="STATEN ISLAND" GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY YEAR DESC, COUNTS DESC')
plote = go.Scatter(x=countisland.toPandas()['Year'],y=countisland.toPandas()['COUNTS'],name='STATEN ISLAND')
py.plot({'data':[plote],'layout':{'title':'Number of Complaints in Staten Island'}},filename='Staten_Island_Count')

#Average Response Time for each 311 call in each Borough
df = spark.sql('SELECT `Created Date`,`BOROUGH`,`Closed Date`,SUBSTRING(`CREATED DATE`,0,4) AS Year,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="MANHATTAN" AND DATEDIFF(`Closed Date`,`Created Date`) >= 0 ORDER BY Year')
df = df.groupby("Year").avg()
df.show()
avgplot = go.Scatter(x=df.toPandas()['Year'],y=df.toPandas()['avg(difference)'],name='MANHATTAN')
py.plot({'data':[avgplot],'layout':{'title':'AVG Response time in MANHATTAN'}},	filename='MANHATTAN')
df = spark.sql('SELECT `Created Date`,`BOROUGH`,`Closed Date`,SUBSTRING(`CREATED DATE`,0,4) AS Year,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="BROOKLYN" AND DATEDIFF(`Closed Date`,`Created Date`) >= 0 ORDER BY Year')
df = df.groupby("Year").avg()
df.show()
avgplot = go.Scatter(x=df.toPandas()['Year'],y=df.toPandas()['avg(difference)'],name='Brooklyn')
py.plot({'data':[avgplot],'layout':{'title':'AVG Response time in Brooklyn'}},	filename='Brooklyn')
df = spark.sql('SELECT `Created Date`,`BOROUGH`,`Closed Date`,SUBSTRING(`CREATED DATE`,0,4) AS Year,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="STATEN ISLAND" AND DATEDIFF(`Closed Date`,`Created Date`) >= 0 ORDER BY Year')
df = df.groupby("Year").avg()
df.show()
avgplot = go.Scatter(x=df.toPandas()['Year'],y=df.toPandas()['avg(difference)'],name='STATEN ISLAND')
py.plot({'data':[avgplot],'layout':{'title':'AVG Response time in STATEN ISLAND'}},	filename='STATEN ISLAND')
df = spark.sql('SELECT `Created Date`,`BOROUGH`,`Closed Date`,SUBSTRING(`CREATED DATE`,0,4) AS Year,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="QUEENS" AND DATEDIFF(`Closed Date`,`Created Date`) >= 0 ORDER BY Year')
df = df.groupby("Year").avg()
df.show()
avgplot = go.Scatter(x=df.toPandas()['Year'],y=df.toPandas()['avg(difference)'],name='QUEENS')
py.plot({'data':[avgplot],'layout':{'title':'AVG Response time in QUEENS'}},	filename='QUEENS')
df = spark.sql('SELECT `Created Date`,`BOROUGH`,`Closed Date`,SUBSTRING(`CREATED DATE`,0,4) AS Year,DATEDIFF(`Closed Date`,`Created Date`) AS difference FROM table WHERE BOROUGH="BRONX" AND DATEDIFF(`Closed Date`,`Created Date`) >= 0 ORDER BY Year')
df = df.groupby("Year").avg()
df.show()
avgplot = go.Scatter(x=df.toPandas()['Year'],y=df.toPandas()['avg(difference)'],name='BRONX')
py.plot({'data':[avgplot],'layout':{'title':'AVG Response time in BRONX'}},	filename='BRONX')



###Distribution for types of Complaints for Each Burough
#Brooklyn
temp = spark.sql('SELECT `Complaint Type`, COUNT(*) as Count,SUBSTRING(`CREATED DATE`,0,4) AS Year FROM table WHERE Borough="BROOKLYN" GROUP BY `Year`,`Complaint Type`')
temp.createOrReplaceTempView("tem")
t1 = spark.sql('SELECT ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Count DESC) AS rownum, `Complaint Type`,`Year`,`Count` FROM tem')
t1.createOrReplaceTempView("tab")
numcombrook = spark.sql('SELECT `Complaint Type`,`Year` FROM tab WHERE rownum=1 ORDER BY Year DESC')
numcombrook.show()
#Queens
temp = spark.sql('SELECT `Complaint Type`, COUNT(*) as Count,SUBSTRING(`CREATED DATE`,0,4) AS Year FROM table WHERE Borough="QUEENS" GROUP BY `Year`,`Complaint Type`')
temp.createOrReplaceTempView("tem")
t1 = spark.sql('SELECT ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Count DESC) AS rownum, `Complaint Type`,`Year`,`Count` FROM tem')
t1.createOrReplaceTempView("tab")
numcombrook = spark.sql('SELECT `Complaint Type`,`Year` FROM tab WHERE rownum=1 ORDER BY Year DESC')
numcombrook.show()
#Staten Island
temp = spark.sql('SELECT `Complaint Type`, COUNT(*) as Count,SUBSTRING(`CREATED DATE`,0,4) AS Year FROM table WHERE Borough="STATEN ISLAND" GROUP BY `Year`,`Complaint Type`')
temp.createOrReplaceTempView("tem")
t1 = spark.sql('SELECT ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Count DESC) AS rownum, `Complaint Type`,`Year`,`Count` FROM tem')
t1.createOrReplaceTempView("tab")
numcombrook = spark.sql('SELECT `Complaint Type`,`Year` FROM tab WHERE rownum=1 ORDER BY Year DESC')
numcombrook.show()
#Manhattan
temp = spark.sql('SELECT `Complaint Type`, COUNT(*) as Count,SUBSTRING(`CREATED DATE`,0,4) AS Year FROM table WHERE Borough="MANHATTAN" GROUP BY `Year`,`Complaint Type`')
temp.createOrReplaceTempView("tem")
t1 = spark.sql('SELECT ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Count DESC) AS rownum, `Complaint Type`,`Year`,`Count` FROM tem')
t1.createOrReplaceTempView("tab")
numcombrook = spark.sql('SELECT `Complaint Type`,`Year` FROM tab WHERE rownum=1 ORDER BY Year DESC')
numcombrook.show()
#Bronx
temp = spark.sql('SELECT `Complaint Type`, COUNT(*) as Count,SUBSTRING(`CREATED DATE`,0,4) AS Year FROM table WHERE Borough="BRONX" GROUP BY `Year`,`Complaint Type`')
temp.createOrReplaceTempView("tem")
t1 = spark.sql('SELECT ROW_NUMBER() OVER (PARTITION BY Year ORDER BY Count DESC) AS rownum, `Complaint Type`,`Year`,`Count` FROM tem')
t1.createOrReplaceTempView("tab")
numcombrook = spark.sql('SELECT `Complaint Type`,`Year` FROM tab WHERE rownum=1 ORDER BY Year DESC')
numcombrook.show()

## Number of Complaints per Person
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/2278906) AS `Complaints Per Person` FROM table where Borough="QUEENS" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1432132) AS `Complaints Per Person` FROM table where Borough="BRONX" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/2582830) AS `Complaints Per Person` FROM table where Borough="BROOKLYN" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/1628701) AS `Complaints Per Person` FROM table where Borough="MANHATTAN" and SUBSTRING(`Created Date`,0,4)=="2010"').show()
spark.sql('SELECT COUNT(*) AS Count,(COUNT(*)/476179) AS `Per Complaints Per Person` FROM table where Borough="STATEN ISLAND" and SUBSTRING(`Created Date`,0,4)=="2010"').show()

#Number of Complaints Distributed 
Totalnumber=spark.sql('SELECT COUNT(*) AS `Number of Complaints`,SUBSTRING(`Created Date`,0,4) AS Year FROM TABLE GROUP BY SUBSTRING(`Created Date`,0,4) ORDER BY Year desc')
Totalnumber.show()

#Counts for Each Complaint Type
countcomplaint=spark.sql('SELECT `Complaint Type`,COUNT(*) AS `Number of Complaints` FROM TABLE GROUP BY `Complaint Type` ORDER BY `Number of Complaints` DESC LIMIT 10')
countcomplaintplot = go.Bar(y=countcomplaint.toPandas()['Number of Complaints'],x=countcomplaint.toPandas()['Complaint Type'],orientation='h')
py.plot({'data':[countcomplaintplot],'layout':{'title':'Complaint Types in New York City','margin':{'l':'180'}}},filename='typecount')
countcomplaint.show()

#Complaint Status
ComplaintStatus=spark.sql('SELECT Status,COUNT(Status) AS Counts FROM TABLE GROUP BY STATUS')
countcomplainplot = go.Bar(y=ComplaintStatus.toPandas()['Counts'],x=ComplaintStatus.toPandas()['Status'],orientation='v')
py.plot({'data':[countcomplainplot],'layout':{'title':'Status of Complaints','margin':{'l':180}}},filename='status')
ComplaintStatus=spark.sql('SELECT Status,COUNT(Status) AS Counts FROM TABLE WHERE Status <>"Closed" GROUP BY STATUS ORDER BY Counts DESC')
countcomplainplot = go.Bar(y=ComplaintStatus.toPandas()['Counts'],x=ComplaintStatus.toPandas()['Status'],orientation='v')
py.plot({'data':[countcomplainplot],'layout':{'title':'Status of Complaints without Closed','margin':{'l':180}}},filename='woClosed')