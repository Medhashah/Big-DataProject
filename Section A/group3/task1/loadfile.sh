echo "script Started"
rm -rf datasets.tsv

hadoop fs -get /user/hm74/NYCOpenData/datasets.tsv
IFS = "\t"
cat datasets.tsv | while read line ; do 
read -a name <<< "$line"
filename="${name}.tsv.gz"
echo $name
spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON task1.py /user/hm74/NYCOpenData/$filename
done 

