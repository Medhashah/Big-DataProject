echo "Script for task 2 started"

hadoop fs -rm -r neighborhood.txt
hadoop fs -put neighborhood.txt
hadoop fs -rm -r city.txt
hadoop fs -put city.txt
hadoop fs -rm -r agency.txt
hadoop fs -put agency.txt
hadoop fs -rm -r carmake.txt
hadoop fs -put carmake.txt
hadoop fs -rm -r colors.txt
hadoop fs -put colors.txt
count=0
tr -s "[[:blank:]'," "[\n*]" < cluster2.txt |
while IFS= read -r word; do
if [ $count == 0 ]
then
count=$count+1
else
spark-submit --conf spark.executor.memoryOverhead=3G --executor-memory 6G task2.py /user/hm74/NYCColumns/$word
fi
done

echo "Script for task 2 ended"
