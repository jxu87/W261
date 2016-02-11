#!/usr/bin/python
## mapper.py
## Author: Jing Xu
## Description: mapper code for HW3.2.2

import sys
import csv
import re

sys.stderr.write("reporter:counter:MapCounter,Instances,1\n")

WORD_RE = re.compile(r"[\w']+") #Compile regex to easily parse complete words

reader = csv.reader(sys.stdin)
# read input from STDIN (standard input)
for line in reader:
    # split the line into words
    issue = line[3]
    words = re.findall(WORD_RE,issue) #create list of words
    for word in words:
        print '%s\t%s' % (word.lower(), 1)
    sys.stderr.write("reporter:counter:MapperCounter,Lines,1\n")