#!/bin/sh

FILES=./tmp_result/*

for f in $FILES
do
    echo "Importing $f file"
    mongoimport -d bigdata -c profile $f
done
