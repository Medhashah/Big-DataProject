#!/usr/bin/env python
# coding: utf-8

# The purpose of this script is to read in a list of dataset json files and merge them into a single task1.json

import os
import json

def cat_json(output_filename, input_filenames):
    with open(output_filename, "w") as outfile:
        first = True
        for infile_name in input_filenames:
            with open(infile_name) as infile:
                if first:
                    outfile.write('{ "datasets": [')
                    first = False
                else:
                    outfile.write(',')
                outfile.write(infile.read())
        outfile.write('] }')

if __name__ == "__main__":
    
    output = "task1.json"
    files = []
    directory = "/home/mva271"

    for file in os.listdir(directory):
        if(file != 'profiling.py' or file != 'run_task1.sh' or file != 'merge_jsons.py'):
            files.append(directory+str(file))

    cat_json(directory+output, files)