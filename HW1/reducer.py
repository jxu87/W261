#!/usr/bin/python
## reducer.py
## Author: Jing Xu
## Description: reducer code for HW1.4

import sys
import ast
import math

spam_emails = 0
total_emails = 0
total_spam_words = 0
total_ham_words = 0
words = ''
unique_words = []
all_emails = []
final_count_dictionary = {}
final_count_dictionary['spam'] = {}
final_count_dictionary['ham'] = {}  
for filenames in sys.argv[1:]: #open each filename in the countfile list
    myfile = open('%s'%filenames, "r")
    for line in myfile.readlines():
        if line[0] == "{":
            count_dictionary = line
            count_dictionary = ast.literal_eval(count_dictionary) #convert dictionary string to dictionary class
            for key in count_dictionary:
                for word in count_dictionary[key]:
                    if word not in final_count_dictionary[key]: final_count_dictionary[key][word] = count_dictionary[key][word]
                    else: final_count_dictionary[key][word] += count_dictionary[key][word]
        else: line = line.split()
        #aggregate counts for all variables of interest
        if line[0] == "spam_emails": 
            spam_emails+=int(line[1])
        elif line[0] == "total_emails":
            total_emails+=int(line[1])    
        elif line[0] == 'total_spam_words':
            total_spam_words+=int(line[1])
        elif line[0] == 'total_ham_words':
            total_ham_words+=int(line[1])
        elif line[0] == 'word': #create variable for search word
            words = line[1:]
        else: #create list of unique words for later use
            for word in line:
                if word not in unique_words: unique_words.append(word)
            all_emails.append(line)
            
prior_spam = float(spam_emails)/float(total_emails) #prior spam = spam emails / total emails
prior_ham = float(total_emails-spam_emails)/float(total_emails) #prior ham = ham emails / total emails

predictions = []

#creation of conditional probability dictionary for all search words in all spam and ham emails
conditional_prob = {}
conditional_prob['spam'] = {}
conditional_prob['ham'] = {}
for word in words:
    if word in final_count_dictionary['spam']: 
        conditional_prob['spam'][word] = (float(final_count_dictionary['spam'][word]) + float(1))/(float(total_spam_words) + float(len(unique_words)))
    else: conditional_prob['spam'][word] = (float(1))/(float(total_spam_words)+float(len(unique_words)))
    if word in final_count_dictionary['ham']:
        conditional_prob['ham'][word] = (float(final_count_dictionary['ham'][word]) + float(1))/(float(total_ham_words) + float(len(unique_words)))
    else: conditional_prob['ham'][word] = (float(1))/(float(total_ham_words)+float(len(unique_words)))
        
for email in all_emails:
    mnb_spam_probability = prior_spam #start of MNB formula to calculate ham probability given search words
    mnb_ham_probability = prior_ham #start of MNB formula to calculate ham probability given search words
    for word in words:
        count_of_word = 0
        for each in email: 
            if word == each: #create count of word
                count_of_word+=1
            mnb_spam_probability *= float(conditional_prob['spam'][word]**count_of_word) #completion of formula for calculating probability of spam given a word
            mnb_ham_probability *= float(conditional_prob['ham'][word]**count_of_word) #completion of formula for calculating probability of ham given a word        
    if mnb_spam_probability > mnb_ham_probability: predictions.append(1) #if probability of spam > ham, prediction of 1 indicates spam
    else: predictions.append(0)

print predictions