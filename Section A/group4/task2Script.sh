module load python/gnu/3.6.5
module load spark/2.4.0
export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON task2.py