#!/usr/bin/python
## mapper.py
## Author: Jing Xu
## Description: mapper code for HW1.4
import sys
import re
import string


spam_emails = 0
total_emails = 0
total_spam_words = 0
total_ham_words = 0
filename = sys.argv[1]
findwords = sys.argv[2].split(' ')
count_dictionary = {}
count_dictionary['spam'] = {}
count_dictionary['ham'] = {}  
emails = open(filename, "r")
for line in emails.readlines():
    line = line.translate(string.maketrans("",""), string.punctuation) #strip punctuation
    email = re.split(r'\t+', line) 
    if len(email) != 4: #skip over email data formatting errors          
        continue
    total_emails+=1
    content = email[0] + email[2] + email[3] #concatenate subject and body sections into one string
    content = re.sub(r'\w*\d\w*', '', content).strip() #strip all words that include a number as these words are unlikely to be predictive
    content = re.sub("\s\s+" , " ", content) #strip all extra whitespaces
    list_content = content.split(' ') #list of each word in line
    if int(email[1]) == 1: #check if the email is spam or not, count instances of word appearing in spam/not-spam emails and total emails
        spam_emails+=1
        for word in findwords:
            word = word.lower()
            for each in list_content:
                if each.lower() == word:
                    if word not in count_dictionary['spam']: count_dictionary['spam'][word] = 1   
                    else: count_dictionary['spam'][word]+=1
                total_spam_words+=1
    else: 
        for word in findwords:
            word = word.lower()
            for each in list_content:
                if each.lower() == word:
                    if word not in count_dictionary['ham']: count_dictionary['ham'][word] = 1   
                    else: count_dictionary['ham'][word]+=1
                total_ham_words+=1    
    print content
    
print "spam_emails", spam_emails
print "total_emails", total_emails
print "total_spam_words", total_spam_words
print "total_ham_words", total_ham_words
print "word", sys.argv[2]
print count_dictionary