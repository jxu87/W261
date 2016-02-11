#!/usr/bin/python
## HW4-2-log_preprocess.py
## Author: Angela Gunn & Jing Xu
## Description: Proprocesses log data on a single node
import sys
import os

if len(sys.argv) < 2:
    print "No input file is passed, Aborting!!!"
    sys.exit(1)

input_file = sys.argv[1]
output_file = input_file + '.pp'

try:
    os.remove(output_file)
except OSError:
    pass

last_visitor = None #set last visitor value to append to output file
with open(input_file, 'r') as f1: #open input file to read
    with open(output_file, 'a') as f2: #open ouput file to write
        for line in f1:
            line = line.strip()
            tokens = line.split(",")
            if len(tokens) == 3 and tokens[0] == 'C': #check that
                last_visitor = tokens[2] 

            if len(tokens) == 3 and tokens[0] == 'V':
                out_line = 'V,{0},C,{1}\n'.format(tokens[1],last_visitor)
                f2.write(out_line)    
    