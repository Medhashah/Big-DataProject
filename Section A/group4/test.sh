#spark submit
module load python/gnu/3.6.5
module load spark/2.4.0
export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

while IFS=$'\t' read filename descrip
do
 	#echo $filename
 	spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON --conf spark.executor.memoryOverhead=3G --executor-memory 6G processScript.py $filename $descrip
done < "sortedLargeTable.tsv"

#spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON processScript.py "sm48-t3s6"