import matplotlib.pyplot as plt
import os
import json

freq_sets = {}
for i in os.listdir("jsons"):
    with open("jsons/" + i, "rb") as f:
        gg = json.load(f)
    for j in gg['columns']:
        temp = []
        for k in j["data_types"]:
            temp.append(k["type"])
        if len(temp) == 0:
        	continue
        for p in range(len(temp)):
            for q in range(p, len(temp)):
                if ','.join(sorted(temp[p:q+1])) in freq_sets:
	                freq_sets[','.join(sorted(temp[p:q+1]))] += 1
                else:
                    freq_sets[','.join(sorted(temp[p:q+1]))] = 1

ones = []
twos = []
threes = []
fours = []
for i in freq_sets:
    key = i.split(",")
    if len(key) == 1:
        ones.append(i)
    elif len(key) == 2:
        twos.append(i)
    elif len(key) == 3:
        threes.append(i)
    elif len(key) == 4:
        fours.append(i)

x_pos = [i for i, _ in enumerate(ones)]
plt.style.use('ggplot')
plt.bar(x_pos, [freq_sets[i] for i in ones], color='green')
plt.xlabel("data type")
plt.ylabel("Frequency of data types")
plt.title("Occurences of data types")
plt.xticks(x_pos, ones)
plt.savefig("counts.png")
print ("ones : ", ones, [freq_sets[i] for i in ones])
print ("twos : ", twos, [freq_sets[i] for i in twos])
print ("threes : ", threes, [freq_sets[i] for i in threes])
print ("fours : ", fours, [freq_sets[i] for i in fours])
