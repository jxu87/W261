#!/usr/bin/python
## reducer.py
## Author: Jing Xu
## Description: reducer code for HW1.4

import sys
import re
import string
import ast

spam_emails = 0
ham_emails = 0
total_spam_words = 0
total_ham_words = 0
findwords = ''
filename = ''
words = {} #creating unique word list

for filenames in sys.argv[1:]: #open each filename in the countfile list
    myfile = open('%s'%filenames, "r")
    for line in myfile.readlines(): #read each line in mapper output
        line = line.split() #split each line into list
        if line[0] == 'SPAM_COUNT': 
            spam_emails+=1 #add 1 to spam_emails
            total_spam_words+=int(line[1]) #add spam words in email to total_spam_words
        elif line[0] == 'HAM_COUNT': 
            ham_emails+=1 #add 1 to ham_emails
            total_ham_words+=int(line[1]) #add ham words in email to total_spam_words
        elif line[0] == 'FINDWORDS': findwords = line[1:] #store findword in memory
        else: 
            word = str(line[1])
            if line[2] == "SPAM": #sort word into SPAM dictionary
                if word not in words: #create new dictionary index for word if not already existing
                    words[word] = {}
                    words[word]['SPAM'] = int(line[3]) #set count of the number of word in spam emails to 1
                else:
                    if 'SPAM' in words[word]: words[word]['SPAM']+=int(line[3]) #add 1 to number of word in spam emails
                    else: words[word]['SPAM'] = int(line[3]) #if word exists in dictionary but not the spam count, create spam count for the word
            else: #sort word into HAM dictionary
                if word not in words: #create new dictionary index for word if not already existing
                    words[word] = {}
                    words[word]['HAM'] = int(line[3]) #set count of the number of word in ham emails to 1            
                else: 
                    if 'HAM' in words[word]: words[word]['HAM']+=int(line[3]) #add 1 to number of word in ham emails
                    else: words[word]['HAM'] = int(line[3]) #if word exists in dictionary but not the ham count, create spam count for the word

for findword in findwords:
    prior_spam = float(spam_emails)/(float(spam_emails)+float(ham_emails)) #prior spam = spam emails / total emails
    prior_ham = float(ham_emails)/(float(spam_emails)+float(ham_emails)) #prior ham = ham emails / total emails
    try: spam_probability = float(words[findword]['SPAM'])/float(total_spam_words) #spam probability is the number of occurrences of word in spam emails / total words in spam emails
    except: spam_probability = 'N/A'
    try: ham_probability = float(words[findword]['HAM'])/float(total_ham_words) #ham probability is the number of occurrences of word in ham emails / total words in non-spam emails
    except: ham_probability = 'N/A'
    print "Class conditional: SPAM,", findword, spam_probability
    print "Class conditional: HAM,", findword, ham_probability
    print findword, "SPAM Prior =", prior_spam
    print findword, "HAM Prior =", prior_ham