import sys
import subprocess
import time

print(sys.argv, len(sys.argv))
start_index = int(sys.argv[1])
end_index = int(sys.argv[2])
for i in range(start_index, end_index, 50):
    mend_index = i+50
    if i+50> end_index:
        mend_index = end_index
    subprocess.run("nohup spark-submit part_1.py {} {} > job_{}_{}.txt & 2>&1".format(i, mend_index, i, mend_index), shell=True)
    time.sleep(5)