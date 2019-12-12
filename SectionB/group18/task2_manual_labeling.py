from fuzzywuzzy import fuzz

task2_datasets = open('cluster3.txt')
dataset_file_names = task2_datasets.readline()[1:-2].split(',')
dataset_file_names = [x.replace("'", "").replace(" ", "").split('.')[0:2] for x in dataset_file_names]

df = spark.read.csv("/user/hm74/NYCOpenData/datasets.tsv",header=False,sep="\t")

for dataset_i, dataset_item in enumerate(dataset_file_names[55:]):
    print("Dataset No:", dataset_i)
    print(dataset_item)
    dataset_file_path = '/user/hm74/NYCOpenData/' + dataset_item[0] + '.tsv.gz'
    dataset_df = spark.read.csv(dataset_file_path, header = True, sep = '\t')
    print("The columns and count of the dataset are:\n", dataset_df.count())
    match_col, match_score = '', 0
    col_list = []
    for col in dataset_df.columns:
        cur_score = fuzz.token_sort_ratio(dataset_item[1], col)
        col_list.append((col, cur_score))
    col_list.sort(key = lambda x: x[1], reverse = True)
    print(col_list)
    #col_name = input("Give column name:\t")
    #dataset_column = dataset_df.select(col_name)
    #dataset_column.sample(False, 0.8, seed = 9).limit(100).show(100, truncate = False)
    proceed = 'n'
    while(proceed != 'y'):
        proceed = input("proceed?\t")
    #break
