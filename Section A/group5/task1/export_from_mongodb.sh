#!/bin/sh

mongoexport --db bigdata -c profile --fields=dataset_name,columns,key_column_candidates --jsonArray |  sed '/"_id":/s/"_id":[^,]*,//g' \
        | python3 -m json.tool> profile.json
