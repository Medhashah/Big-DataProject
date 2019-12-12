#!/usr/bin/env bash
source ~/.bashrc
module load python/gnu/3.6.5
module load spark/2.4.0
spark-submit \
  --executor-memory 6G \
  --driver-memory 4G \
  --conf spark.executor.memoryOverhead=6G \
  --conf spark.storage.memoryFraction=0.5 \
  --conf spark.shuffle.memoryFraction=0.3 \
  task1.py