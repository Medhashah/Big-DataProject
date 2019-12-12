#!/bin/sh

FILE=main.py
FILE1=rdd_util.py
OUTPUT=project_output

export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'


#hadoop fs -rm -r ${OUTPUT}.out


spark-submit --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./env/my-environment/bin/python \
--conf spark.executor.memoryOverhead=3G --executor-memory 6G \
--archives my-environment.zip#env \
--py-files ${FILE1} ${FILE}

#hfs -get ${OUTPUT}.out
#
#hfs -getmerge ${OUTPUT}.out ${OUTPUT}.txt