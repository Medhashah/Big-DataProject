import sys
def replace(argv1,argv2):
	f = open("cluster1.txt","rt")
	content = f.read()
	content = content.replace(argv1,argv2)
	fout = open("cluster1.txt","wt")
	fout.write(content)
	fout.close()
