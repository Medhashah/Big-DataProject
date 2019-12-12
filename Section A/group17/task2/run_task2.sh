#!/bin/sh

set -e

PYTHON=/share/apps/python/3.4.4/bin/python
$PYTHON task2_prepare.py columns.txt colstxt

# Already exists in HDFS
#hadoop fs -put colstxt /user/hl3475/final_project/task2/colstxt

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'
spark-submit task2_itemcount.py columns.txt colstxt_itemcount

# Require Python modules: names-dataset validators sklearn=0.19.1 scipy=1.0.0
PYTHON_LOCAL=/home/hl3475/task2/env/python/3.4.4/bin/python
$PYTHON_LOCAL task2_detect_semantics.py columns.txt colstxt_itemcount semantics
$PYTHON_LOCAL task2_evaluate.py columns_type.txt columns_type_predicted.txt