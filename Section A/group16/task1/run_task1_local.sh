#!/bin/sh

set -e

PYTHON=/share/apps/python/3.4.4/bin/python
mkdir -p output
for f in $(cat datasets.list)
do 
    cnt=`ls -l output | grep -v total | wc -l`
    echo "Start to process No.$cnt: $f"
    $PYTHON task1_local.py ../NYCOpenData/$f ./output/${f}.json
done
$PYTHON task1_merge.py datasets.txt output