import numpy as np
import matplotlib.pyplot as plt
import csv

def subcategorybar(X, vals, width=0.8):
    n = len(vals)
    _X = np.arange(len(X))
    for i in range(n):
        plt.bar(_X - width/2. + i/float(n)*width, vals[i],
                width=width/float(n), align="edge")
    plt.xticks(_X, X)

labels = []
recalls = []
precisions = []
with open("plot.csv", 'r') as csvfile:
    csvreader = csv.reader(csvfile)

    for row in csvreader:
        labels.append(row[0])
        recalls.append(row[1])
        precisions.append(row[2])

print (labels)
print (recalls)
print (precisions)

subcategorybar(labels, recalls)

plt.show()