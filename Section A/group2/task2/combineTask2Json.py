import sys
import string
import json
import os

path = r"./task2out/"

list_dirs = os.walk(path) 
cnt = 0
for root, dirs, files in list_dirs: 
    fNum = len(files)
    #Combine json files
    with open('./task2.json', 'w') as cjsf:
        for f in files: 
            cnt += 1
            # sys.stdout.write("\rcurrent step: {}/{}".format(cnt,fNum))
            # sys.stdout.flush()
            fp = os.path.join(root, f)
            with open(fp,'r') as jsf:
                jstr = jsf.read()
                cjsf.write(jstr+'\n')