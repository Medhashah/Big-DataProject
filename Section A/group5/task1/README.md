# Task 1: Generic Profiling

To run the program, run the `run.sh`.

In the `main.py`, the process will loop through the file `file_list/file_list_sorted.csv` and call `rdd_util.generate_meta` on each dataset to generate the output. And it will create a temporay file for each dataset in `tmp_result` folder.

We import them into MongoDB by using the script `import_to_mongodb.sh` and in the end export them with the script `export_from_mongodb.sh`.

The `generate_result.sh` is used to generate the right format for the filnal output.


