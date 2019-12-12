from os import listdir
from os.path import isfile, join

mypath = "../raw_output/"

processed = [f for f in listdir(mypath) if isfile(join(mypath, f)) and f.split('.')[-1] == 'json']

f = open('dataset_index.txt', 'r')

filenames = []
original_file = []

for x in f:
    line = x
    line = line.strip()
    line = line.split(',', 2)
    index = int(line[0])
    filename = line[1].strip().split('/')[-1].split('.')[0] + '.json'
    # unprocessed files
    if filename not in processed: original_file.append(x)

f.close()

f = open('../remaining_data/remaining_data.txt', 'w')
for line in original_file:
    f.write(line)
f.close()