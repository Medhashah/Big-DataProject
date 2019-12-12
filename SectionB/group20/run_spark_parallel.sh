#!/bin/bash
list_file=$1
chunk_size=$2

usage()
{
    echo "Usage: ./run_spark_parallel.sh list_file chunk_size"
}

error()
{
    echo -e "\e[01;91mERROR: $1\e[00m"
    usage
    exit 1
}

[ $# -eq 0 ] && error "Insufficient arguments"
[ -z $chunk_size ] && echo "Defaulting chunk_size=4" && chunk_size=4
[ -z $list_file ] && echo "File containing a list of dataset names has to supplied" #TODO: Automate this part

log_file="result_`date +%Y-%m-%d`.out"
rm $log_file*
echo "Logging into $log_file"
line_count=`cat $list_file | wc -l`
#echo "line_count=$line_count"
start_line=0
end_line=0
iter_count=0

while [ $end_line -lt $line_count ]
do
    iter_count=0
    start_line=$((end_line + 1))
    end_line=$((end_line + chunk_size))
    [ $end_line -gt $line_count ] && end_line=$line_count
   # echo "start_line=$start_line"
   # echo "end_line=$end_line"
    for i in `cat $list_file | sed -n "${start_line},${end_line}p"`
    do
        iter_count=$((iter_count + 1))
        echo "Submitting job for $i - Logging to ${log_file}_${iter_count}"
        # echo "Hello" > "${log_file}_${iter_count}"
        # ./test.sh $i &
        spark-submit --driver-memory 8g --executor-memory 8g --conf spark.pyspark.python=$PYSPARK_PYTHON template.py $i >> "${log_file}_${iter_count}" 2>&1 &
    done
    wait
done
exit 0
