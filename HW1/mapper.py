#!/usr/bin/python
## mapper.py
## Author: Jing Xu
## Description: mapper code for HW1.2
import sys
import re
import string

filename = sys.argv[1] #read in first argument as the emails to be parsed
findwords = sys.argv[2] #read in second argument as the word to be counted
print "FINDWORD", findwords

with open (filename, "r") as emails:
    for email in emails.readlines(): #read each line in enronemail file, each corresponding to a single email sent
        words = email.translate(string.maketrans("",""), string.punctuation) #strip all punctuation
        words = words.split() #convert line into list of words
        for word in words:
            print word, 1
