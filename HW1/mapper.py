#!/usr/bin/python
## mapper.py
## Author: Jing Xu
## Description: mapper code for HW1.4
import sys
import re
import string

filename = sys.argv[1]
findwords = sys.argv[2]
emails = open(filename, "r")
print "FINDWORDS", findwords

for line in emails.readlines():
    email_id = line.split('\t')[0]
    line = line.translate(string.maketrans("",""), string.punctuation) #strip punctuation
    email = re.split(r'\t+', line) #strip words that include any numbers
    if len(email) != 4: #skip over email data formatting errors          
        continue
    content = email[2] + email[3] #concatenate subject and body sections into one string
    content = re.sub(r'\w*\d\w*', '', content).strip() #strip all words that include a number as these words are unlikely to be predictive
    content = re.sub("\s\s+" , " ", content) #strip all extra whitespaces
    list_content = content.split(' ') #list of each word in line
    if int(email[1]) == 1: #check if the email is spam or not, count instances of word appearing in spam/not-spam emails and total emails
        print "SPAM_COUNT", len(list_content) #emit key of SPAM_COUNT along with a word count value for later calculation of total SPAM words
        for word in list_content:
            print email_id, word.lower(), "SPAM", list_content.count(word) #emit email_id key, word, class, and count
    else: 
        print "HAM_COUNT", len(list_content) #emit key of HAM_COUNT along with a word count value for later calculation of total HAM words
        for word in list_content:
            print email_id, word.lower(), "HAM", list_content.count(word) #emit email_id key, word, class, and count
    