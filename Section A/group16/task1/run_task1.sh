#!/bin/sh

set -e

module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'
mkdir -p output
spark-submit task1.py datasets.list output 2>>spark.log

$PYTHON=/share/apps/python/3.4.4/bin/python
$PYTHON task1_merge.py datasets.txt output