
fileName = "qu8g-sxqf_First_Name.out"
label = "First name"
f1 = open(fileName+"/part-00000","r")
f2 = open(fileName+"/part-00001","r")
line = f1.readline()
line = f1.readline()
item_arr = []
while line:
    value = str(line.strip().split(",")[len(line.split(","))-1])
    key = line.replace(","+value,"").strip()
    key = "'"+key+"'"
    result = "'"+label+"', "+key+", "+value
    item_arr.append(result)
    line = f1.readline()

line = f2.readline()
while line:
    value = str(line.strip().split(",")[len(line.split(","))-1])
    key = line.replace(value,"").strip()
    key = "'"+key+"'"
    result = "'"+label+"', "+key+", "+value
    item_arr.append(result)
    line = f2.readline()

f1.close()
f2.close()
output_file = "./output/"+fileName.replace(".out",".txt")
print(output_file)
out = open(output_file,"w+")
for item in item_arr:
    out.write(item)
    out.write("\n")
out.close()

