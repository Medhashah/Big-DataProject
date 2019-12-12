#Requires python version >= 2.7
import csv

with open("test.csv","r") as fin, open("newtest.csv","w") as fout:
	r = csv.reader(fin,quotechar='"', delimiter=',',quoting=csv.QUOTE_ALL,skipinitialspace=True)
	w = csv.writer(fout, delimiter=',', quotechar='"', quoting=csv.QUOTE_ALL,skipinitialspace=True)
	for i in r:
		if "2017" in i[1] or "2018" in i[1] or "2019" in i[1] or "Created" in i[1]:
			# Create date, close date, complaint type, borough, lat, long
			w.writerow([i[1],i[2],i[5],i[25],i[38],i[39]])