#!/bin/sh

#FILE2=$(<task1_result.tpl)
#FILE1=$(<profile.json)
#echo "${FILE2/<RESULT>/$FILE1}" > result.json

sed -f script.sed task1_result.tpl > task1_result.json