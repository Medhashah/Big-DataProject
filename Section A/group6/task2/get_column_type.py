from os import listdir
from os.path import isfile, join
import pprint
data = {}
main_directory = "./labelled_data/"
# get all files in the labelled_data folder
onlyfiles = [f for f in listdir(main_directory) if isfile(join(main_directory, f))]
for i in range(0,len(onlyfiles)):
    item = main_directory + onlyfiles[i] 
    print("this is the " + str(i)+" item out of "+str(len(onlyfiles))+" items")
    fileName = item # file name
    item = item.replace(".txt","").strip()
    vp = item.split("_",1)
    table = vp[0] # table name
    column = vp[1] # column name

    # opening and reading file
    fp = open(fileName,"r")
    line = fp.readline()
    key_list = []
    while line:
        l_key = line.split(",")[0].replace("'","").strip()
        if l_key == "Business Name":
            l_key = "Business name"
        if "name" in l_key:
            l_key = "Person name"
        if l_key == "Letter":
            l_key = "Other"
        if l_key == "Park":
            l_key = "Parks/Playgrounds"
        if l_key == "Phone number":
            l_key = "Phone Number"
        if l_key == "School levels":
            l_key = "School Levels"
        if l_key == "other":
            l_key = "Other"

        if l_key not in key_list:
            key_list.append(l_key) # add to the key_list array
        line = fp.readline()

    for kl_key in key_list: # add keys to data dictionary
        if kl_key == "Business Name":
            kl_key = "Business name"
        if "name" in kl_key:
            kl_key = "Person name"
        if kl_key == "Letter":
            kl_key = "Other"
        if kl_key == "Park":
            kl_key = "Parks/Playgrounds"
        if kl_key == "Phone number":
            kl_key = "Phone Number"
        if kl_key == "School levels":
            kl_key = "School Levels"
        if kl_key == "other":
            kl_key = "Other"
        if kl_key in data.keys():
            data[kl_key] += 1
        else:
            data[kl_key] = 1
    fp.close()
# output results
pp = pprint.PrettyPrinter(indent=4)
pp.pprint(data)

