# NYC-bigdata-profiling-analysis

Data sources:https://opendata.cityofnewyork.us/

We focus on more than 1900 datasets.

# Our goal
1. Profile those datasets correctly and efficiently. (data profiling and cleaning)
2. Labeling 274 targeted datasets for further analysis
3. Fancy work to be continuing...


# infrastructure
Apache Hadoop

Apache spark

# More details will be updated

Run task2_semantic.py for main task, will generate results folder, containing all json files for each datasets

Run stat.py for generating score.json,including precision and recall value

Run generate_result.py for generating task2.json, and task2-manual-labels.json

Colname_match_list.txt is a list of true column name along with given column name for each dataset

task2_true_label.xlsx is manually labeled true type for each dataset
