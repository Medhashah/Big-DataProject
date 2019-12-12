#!/bin/bash
#there is a issue with how python reads stdout need to set encoding to avoid unicode error on df.select()
#https://issues.apache.org/jira/browse/SPARK-11772
export PYTHONIOENCODING=utf8