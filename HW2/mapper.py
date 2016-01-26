#!/usr/bin/python
#HW 2.5 - Mapper function
#This mapper function will send one line for every instance of every word to the reducer. 

import sys
sys.path.append('.')
import re

WORD_RE = re.compile(r"[\w']+") #Compile regex to easily parse complete words

allowed_words = []

thefile = open('allowed_words', 'r')
for line in thefile:
    line = line.strip()
    allowed_words.append(line)

for line in sys.stdin:
    fields=line.split('\t') #parse line into separate fields
    if len(fields) == 4: #check if email data formatted correctly
        subject_and_body=" ".join(fields[-2:]).strip()#parse the subject and body fields from the line, and combine into one string
        words=re.findall(WORD_RE,subject_and_body) #create list of words
        for word in words:
            #This flag indicates to the reducer that a given word should be considered
            #by the reducer when calculating the conditional probabilities
            if word in allowed_words:
                #This will send one row for every word instance to the reducer.
                print fields[0]+'\t'+fields[1]+'\t'+word