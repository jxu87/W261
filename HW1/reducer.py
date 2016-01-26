#!/usr/bin/python
## reducer.py
## Author: Jing Xu
## Description: reducer code for HW1.2

import sys

findword = None
words = {} #creating unique word list

for filenames in sys.argv[1:]: #open each filename in the countfile list
    myfile = open('%s'%filenames, "r")
    for line in myfile.readlines(): #read each line in mapper output
        line = line.split() #split each line into list
        if line[0] == "FINDWORD": findword = line[1]
        else:
            for word in line:
                if word not in words: words[word] = 1
                else: words[word]+=1

print words[findword]