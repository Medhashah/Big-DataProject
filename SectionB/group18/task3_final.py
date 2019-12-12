''' Execute these to prep and start the PySpark shell
module load python/gnu/3.6.5
module load spark/2.4.0
pyspark
'''

######################### Task 3 ############################
import json

dataset = spark.read.csv("/user/hm74/NYCOpenData/erm2-nwe9.tsv.gz", header = True, sep = "\t")

dataset.printSchema()
'''
root
 |-- Unique Key: string (nullable = true)
 |-- Created Date: string (nullable = true)
 |-- Closed Date: string (nullable = true)
 |-- Agency: string (nullable = true)
 |-- Agency Name: string (nullable = true)
 |-- Complaint Type: string (nullable = true)
 |-- Descriptor: string (nullable = true)
 |-- Location Type: string (nullable = true)
 |-- Incident Zip: string (nullable = true)
 |-- Incident Address: string (nullable = true)
 |-- Street Name: string (nullable = true)
 |-- Cross Street 1: string (nullable = true)
 |-- Cross Street 2: string (nullable = true)
 |-- Intersection Street 1: string (nullable = true)
 |-- Intersection Street 2: string (nullable = true)
 |-- Address Type: string (nullable = true)
 |-- City: string (nullable = true)
 |-- Landmark: string (nullable = true)
 |-- Facility Type: string (nullable = true)
 |-- Status: string (nullable = true)
 |-- Due Date: string (nullable = true)
 |-- Resolution Description: string (nullable = true)
 |-- Resolution Action Updated Date: string (nullable = true)
 |-- Community Board: string (nullable = true)
 |-- BBL: string (nullable = true)
 |-- Borough: string (nullable = true)
 |-- X Coordinate (State Plane): string (nullable = true)
 |-- Y Coordinate (State Plane): string (nullable = true)
 |-- Open Data Channel Type: string (nullable = true)
 |-- Park Facility Name: string (nullable = true)
 |-- Park Borough: string (nullable = true)
 |-- Vehicle Type: string (nullable = true)
 |-- Taxi Company Borough: string (nullable = true)
 |-- Taxi Pick Up Location: string (nullable = true)
 |-- Bridge Highway Name: string (nullable = true)
 |-- Bridge Highway Direction: string (nullable = true)
 |-- Road Ramp: string (nullable = true)
 |-- Bridge Highway Segment: string (nullable = true)
 |-- Latitude: string (nullable = true)
 |-- Longitude: string (nullable = true)
 |-- Location: string (nullable = true)
'''

dataset.createOrReplaceTempView("complaints")

query = "SELECT Borough, `Complaint Type`, COUNT(*) AS NumComplaints FROM complaints GROUP BY Borough, `Complaint Type`"
complaint_count = spark.sql(query)
complaint_count.createOrReplaceTempView("complaint_count")
query = "SELECT * FROM complaint_count"
spark.sql(query).show()
complaint_count.printSchema()
complaint_count.cache()

query = "SELECT Borough, `Complaint Type`, NumComplaints FROM complaint_count WHERE Borough = 'BRONX' ORDER BY NumComplaints DESC LIMIT 10"
bronx_complaints = spark.sql(query)
bronx_complaints.cache()
bronx_complaints.createOrReplaceTempView("bronx_complaints")
spark.sql("SELECT * FROM bronx_complaints").show()

query = "SELECT Borough, `Complaint Type`, NumComplaints FROM complaint_count WHERE Borough = 'BROOKLYN' ORDER BY NumComplaints DESC LIMIT 10"
brooklyn_complaints = spark.sql(query)
brooklyn_complaints.cache()
brooklyn_complaints.createOrReplaceTempView("brooklyn_complaints")
spark.sql("SELECT * FROM brooklyn_complaints").show()

query = "SELECT Borough, `Complaint Type`, NumComplaints FROM complaint_count WHERE Borough = 'MANHATTAN' ORDER BY NumComplaints DESC LIMIT 10"
manhattan_complaints = spark.sql(query)
manhattan_complaints.cache()
manhattan_complaints.createOrReplaceTempView("manhattan_complaints")
spark.sql("SELECT * FROM manhattan_complaints").show()

query = "SELECT Borough, `Complaint Type`, NumComplaints FROM complaint_count WHERE Borough = 'QUEENS' ORDER BY NumComplaints DESC LIMIT 10"
queens_complaints = spark.sql(query)
queens_complaints.cache()
queens_complaints.createOrReplaceTempView("queens_complaints")
spark.sql("SELECT * FROM queens_complaints").show()

query = "SELECT Borough, `Complaint Type`, NumComplaints FROM complaint_count WHERE Borough = 'STATEN ISLAND' ORDER BY NumComplaints DESC LIMIT 10"
staten_complaints = spark.sql(query)
staten_complaints.cache()
staten_complaints.createOrReplaceTempView("staten_complaints")
spark.sql("SELECT * FROM staten_complaints").show()

query = "SELECT Borough, `Complaint Type`, `Incident Zip`, SUBSTRING(`Created Date`, 7, 4) AS Year, COUNT(*) AS NumComplaints FROM complaints GROUP BY Borough, `Incident Zip`, Year, `Complaint Type`"
borough_zip_year_count = spark.sql(query)
borough_zip_year_count.cache()
borough_zip_year_count.createOrReplaceTempView("borough_zip_year_count")

query = "SELECT `Complaint Type`, `Incident Zip`, Year, NumComplaints FROM borough_zip_year_count WHERE `Incident Zip` = '11201' AND Year IN (2010, 2012, 2014, 2016, 2018) AND `Complaint Type` IN ('Noise - Residential', 'Street Condition')" # Tandon
spark.sql(query).show()

query = "SELECT `Complaint Type`, `Incident Zip`, Year, NumComplaints FROM borough_zip_year_count WHERE `Incident Zip` = '10029' AND Year IN (2010, 2012, 2014, 2016, 2018) AND `Complaint Type` IN ('Noise - Residential', 'Street Condition')" # East Harlem
spark.sql(query).show()

query = "SELECT `Complaint Type`, `Incident Zip`, Year, NumComplaints FROM borough_zip_year_count WHERE `Incident Zip` = '10465' AND Year IN (2010, 2012, 2014, 2016, 2018) AND `Complaint Type` IN ('Noise - Residential', 'Street Condition')" # Southeast Bronx
spark.sql(query).show()
