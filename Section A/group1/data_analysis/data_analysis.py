#!/usr/bin/env python
# coding: utf-8


import os
import sys
import subprocess
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
import pyspark.sql.functions as F

sc = SparkContext()
spark = SparkSession(sc)    #initialize the spark context

def get_zipcodes(path):
    
    
    schools = spark.read.format('csv').option("delimiter", "\t").option("header", "true").option("inferschema", "true").csv(str(path + '/' + '6jad-5sav.tsv.gz'))  #dataset contains information about number of students graduated from a scholl with the demographic information.
    
    #schools.show(2)
    
    schools.createOrReplaceTempView("schools")
    
    location = spark.read.format('csv').option("delimiter", "\t").option("header", "true").option("inferschema", "true").csv(path + '/' + 'n3p6-zve2.tsv.gz')#locations of the school along with the zipcodes
    
    location.createOrReplaceTempView("location")
    
    r1 = spark.sql('select DBN, Demographic, SUM(`Total Cohort Num`) as total_cohort, \
           SUM(`Total Grads Num`) as total_grad from schools \
          group by 1,2')
    r1.createOrReplaceTempView("r1")    #find the total students graduated in each school in all cohorts
    
    r2 = spark.sql('select dbn, postcode from location \
           where postcode is not NULL')        #find the zip code for each school
    r2.createOrReplaceTempView("r2")
    
    r3 = spark.sql('select r2.postcode, r1.DBN, r1.Demographic, \
           r1.total_cohort, r1.total_grad \
          from r1 \
          join r2 \
          on r1.DBN = r2.dbn \
          where total_cohort is not NULL and total_grad is not NULL')  #join the two tables
    r3.createOrReplaceTempView("r3")
    
    r4 = spark.sql('select postcode, Demographic, \
          SUM(total_cohort) as total_cohort, \
          SUM(total_grad) as total_grad, \
          (SUM(total_grad)/SUM(total_cohort)) as grad_per \
          from r3 \
          group by 1,2 \
          order by 1,2')
    r4.createOrReplaceTempView("r4")     #finding the percentage number of students graduate per demographic group per zipcode
    
    r5 = spark.sql('select postcode, \
            MAX(case when Demographic = "Asian" then grad_per else NULL end) as Asian_per, \
            MAX(case when Demographic = "White" then grad_per else NULL end) as White_per, \
            MAX(case when Demographic = "Black" then grad_per else NULL end) as Black_per, \
            MAX(case when Demographic = "Hispanic" then grad_per else NULL end) as Hispanic_per \
            from r4 \
            group by 1 \
            order by 1')
    r5.createOrReplaceTempView("r5")   #pivoting the table to see graduation rates for different demographic groups for each zipcode
    
    #r5.show(2)
    
    r5.write.csv('A.csv')
    
    r6 = spark.sql('select postcode, \
            ROUND(ABS(Asian_per - White_per), 3) as A_W, \
            ROUND(ABS(Asian_per - Black_per), 3) as A_B, \
            ROUND(ABS(Asian_per - Hispanic_per), 3) as A_H, \
            ROUND(ABS(White_per - Black_per), 3) as W_B, \
            ROUND(ABS(White_per - Hispanic_per), 3) as W_H, \
            ROUND(ABS(Black_per - Hispanic_per), 3) as B_H \
            from r5')
    r6.createOrReplaceTempView("r6")            #finding the difference between graduation rates of different demographic groups for each zipcode
    
    #r6.show(2)
    
    r6.write.csv('B.csv')
    
    r7 = spark.sql('select postcode, greatest(A_W, A_B, A_H, W_B, W_H, B_H) as g_per \
            from r6 \
            order by greatest(A_W, A_B, A_H, W_B, W_H, B_H) desc')
    r7.createOrReplaceTempView("r7")              # find the zip codes with greatest differences 
    
    r8 = spark.sql('select r7.postcode, r7.g_per, \
            case when r7.g_per = r6.A_W then "A_W" \
            when r7.g_per = r6.A_B then "A_B" \
            when r7.g_per = r6.A_H then "A_H" \
            when r7.g_per = r6.W_B then "W_B" \
            when r7.g_per = r6.W_H then "W_H" \
            when r7.g_per = r6.B_H then "B_H" \
            else NULL end as grp \
          from r7 \
          join r6 \
          on r6.postcode = r7.postcode \
          order by g_per desc')
    r8.createOrReplaceTempView("r8")           #for the zipcodes find which of the demongraphic groups have the max difference
    
    #r8.show(2)
    
    r8.write.csv('C.csv')                   #get a list if zip codes with greatest difference between any of the demograhic groups
    
    print('Categories with maximum disparity')
    
    spark.sql('select grp, count(*) from r8 group by 1 order by count(*) desc').show()  #rank the pairs of demongraphic groups by the number of zipcodes that that highest difference between those 2 groups
    
path = str(sys.argv[1])
    
get_zipcodes(path)

sc.stop()

