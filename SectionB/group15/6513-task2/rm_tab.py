import numpy as np
from numpy import array
import random
from matplotlib import pyplot as plt

if __name__ == "__main__":
	res = ""
	with open('file_with_size.csv') as file:
		file_lines = file.readlines()
	for i in range(len(file_lines)):
		res += file_lines[i].split(' ')[1].strip() + '\n'

	fh = open("clean.txt", 'w') 
	fh.write(res) 

	#print(res)
	# data = np.random.normal(10000, 20, 10000)
	# data = array([4944,   88164 ,   6084, 1901658])
	# print(data)
	# bins = np.arange(0, 2000000, 100000) # fixed bin size

	# plt.xlim([min(data)-400000, max(data)+400000])

	# plt.hist(data, bins=bins, alpha=0.5)
	# plt.title('Random Gaussian data (fixed bin size)')
	# plt.xlabel('variable X (bin size = 5)')
	# plt.ylabel('count')

	# plt.show()




	# 	for file_name in file.readline().split(",")

	#     origins = [file_name.strip()[1:-1] for file_name in file.readline().split(",")]




	# spark = SparkSession \
	#     .builder \
	#     .appName("task 2") \
	#     .config("spark.some.config.option", "some-value") \
	#     .getOrCreate()
	# # get file and dir
	# # /user/hm74/NYCOpenData/c284-tqph.tsv.gz
	# mkdir("./task2_data")
	# data_dir = "/user/hm74/NYCColumns/"
	# files = init_files()
	# count = 1
	# # find column dataset
	# for file in files:
	#     full_file = data_dir + file
	#     print(full_file)

