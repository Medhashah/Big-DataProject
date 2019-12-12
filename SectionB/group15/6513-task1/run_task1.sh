#!/usr/bin/env bash
source ~/.bashrc
module load python/gnu/3.6.5
module load spark/2.4.0
spark-submit --conf spark.pyspark.python=/share/apps/python/3.6.5/bin/python --conf spark.executor.memoryOverhead=3G --executor-memory 6G task1.py $1
