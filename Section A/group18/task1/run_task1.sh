# Setup:
# Change scratch location NETID to individual using it
# Make sure to update location of file list
# File was run on NYU DUMBO HPC

# To Use:
# Comment out the line labeled "2" and uncomment the two lines labeled "1". Running these will get all of the files and put them on the Hadoop server
# Comment out the lines labeled "1" and uncomment the line labeled "2". Running this will loop through a list of files and run them with spark.

# Note: once the datasets are put on hadoop server, they can be physically deleted from scratch

module load python/gnu/3.6.5
module load spark/2.4.0
export PYSPARK_PYTHON='/share/apps/python/3.6.5/bin/python'
export PYSPARK_DRIVER_PYTHON='/share/apps/python/3.6.5/bin/python'

########## 1 ##########
# /usr/bin/hadoop fs -get "/user/hm74/NYCOpenData" "/scratch/mva271"
#######################

input="/scratch/mva271/NYCOpenData/files.txt"
while IFS= read -r line
do
	########## 1 ##########
	# file_path = "/scratch/mva271/NYCOpenData/" + "$line"
	# /usr/bin/hadoop fs -put "$file_path"
	#######################

	########## 2 ##########
	spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON profiling.py "$line"
    #######################

done < "$input"

# Merge all of the json files together into a single "task1.json"
python merge_jsons.py