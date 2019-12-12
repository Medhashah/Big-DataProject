module load python/gnu/3.4.4
module load spark/2.2.0
export PYSPARK_PYTHON='/share/apps/python/3.4.4/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.4.4/bin/python'

# call as: ./test.sh inFile supp conf prot outDir
# for example: ./test.sh /user/jds405/HW2data/PP_small.csv 400 0.4 1.2 $HOME/hw2/res1
# for example: ./test.sh /user/jds405/HW2data/PP_small.csv 500 0.55 1.1 $HOME/hw2/res2

spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON $1 $2 $3



