import datetime
import re
import math


#QL: Get all file names by command line: hfs -ls /user/hm74/NYCOpenData > raw.txt
#We get 1901 string like belowing
#"-rw-rwxr-x+  3 hm74 users       1648 2019-10-04 17:24 /user/hm74/NYCOpenData/2232-dj5q.tsv.gz"
file = open("raw.txt","r")
#Get the substring which aftet "/" mark
p = r"/.*"
pattern = re.compile(p)
#We get string like this 
#"/user/hm74/NYCOpenData/2232-dj5q.tsv.gz"
file_list = [pattern.findall(line)[0] for line in file.readlines()]


#store the results into a new txt file
file = open("filenames.txt","w")
for i in file_list:
	file.write(i+"\n")
file.close()

#Chop into 19 txt files, each has 100 file names.
for i in range(19):
	index = (i+1)*100
	file = open("filename"+str(index)+".txt","w")
	if i!=18:
		for j in range(index-100,index):
			file.write(file_list[j]+"\n")
	else:
		for j in range(index-100,len(file_list)):
			file.write(file_list[j]+"\n")
	file.close()





