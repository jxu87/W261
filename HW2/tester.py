import sys
import re

WORD_RE = re.compile(r"[\w']+") # compile regex to easily parse complete words

findwords = 'assistance'
    
for line in sys.stdin:
    fields=line.split('\t') #parse line into separate fields
    if len(fields) == 4: #check if email data formatted correctly
        subject_and_body=" ".join(fields[-2:]).strip()#parse the subject and body fields from the line, and combine into one string
        words=re.findall(WORD_RE,subject_and_body) #create list of words
        for word in words:
            #This flag indicates to the reducer that a given word should be considered
            #by the reducer when calculating the conditional probabilities
            flag=0
            if word in findwords:
                flag=1 
            #This will send one row for every word instance to the reducer.
            print fields[0]+'\t'+fields[1]+'\t'+word+'\t1\t'+str(flag)