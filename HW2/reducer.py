#!/usr/bin/python
#HW 2.4 - Reducer function
#The reducer maintains two associative arrays. 
#The first stores information about each word, including how 
#many times it appears in spam and ham messages, as well as if it's been flagged in the mapper. 
#The  second stores information about emails, including whether it is marked as spam, as well as a list of 
#words it contains. 
#Once all the data has arrived from the mappers, the array containing words is updated with the calculated 
#conditional probabilities of spam and ham. At this point, the model is "trained". Finally, these 
#conditional probabilities are reapplied to the word lists associated with each email to make the 
#final spam/ham classification as an output file. There is also a count for each zero probability calculation
#for each class. 

from __future__ import division #Python 3-style division syntax is much cleaner
import sys
from math import log
 
words={}
emails={}
spam_email_count=0 #number of emails marked as spam
spam_word_count=0 #number of total (not unique) words in spam emails
ham_word_count=0 #number of total (not unique) words in ham emails
for line in sys.stdin:
    #parse the incoming line
    result=line.split("\t")
    email=result[0]
    spam=int(result[1])
    word=result[2]
    #initialize storage for word/email data
    if word not in words.keys():
        words[word]={'ham_count':0,'spam_count':0}
    if email not in emails.keys():
        emails[email]={'spam':spam,'word_count':0,'words':[]}
        if spam==1:
            spam_email_count+=1
    #store word data 
    if spam==1:
        words[word]['spam_count']+=1
        spam_word_count+=1
    else:
        words[word]['ham_count']+=1
        ham_word_count+=1
    #store email data 
    emails[email]['words'].append(word)
    emails[email]['word_count']+=1
 
#Calculate stats for entire corpus
prior_spam=spam_email_count/len(emails)
prior_ham=1-prior_spam
vocab_count=len(words)#number of unique words in the total vocabulary
spam_zero_prob_count = 0 #count of number of zero probability words for all spam emails
ham_zero_prob_count = 0 #count of number of zero probability words for all spam emails

for k,word in words.iteritems():
    #Compute conditional probabilities WITH Laplace smoothing
    word['p_spam']=(word['spam_count']+1)/(spam_word_count+vocab_count) #add 1 to numerator, vocab_count to denominator
    if word['p_spam']==0: spam_zero_prob_count+=1
    word['p_ham']=(word['ham_count']+1)/(ham_word_count+vocab_count) #add 1 to numerator, vocab_count to denominator
    if word['p_ham']==0: ham_zero_prob_count+=1

correct_predictions = 0
incorrect_predictions = 0
spam_log_posterior_probabilities = [] #keep list of p_spam probabilities for histogram
ham_log_posterior_probabilities = [] #keep list of p_ham probabilities for histogram

#At this point the model is now trained, and we can use it to make our predictions
for j,email in emails.iteritems():    
    p_spam=log(prior_spam) #log of prior_spam
    p_ham=log(prior_ham) #log of prior_ham
    for word in email['words']:
        if words[word]['spam_count'] != 0 and words[word]['ham_count'] != 0: #exclude conditional probabilities for any word where count = 0 for either classification
            try:
                p_spam+=log(words[word]['p_spam']) #log version calculation of p_spam
            except ValueError:
                continue #This means that words that do not appear in a class will use the class prior
            try:
                p_ham+=log(words[word]['p_ham']) #log version calculation of p_ham
            except ValueError:
                continue          
    if p_spam>p_ham: #predict class
        spam_pred=1
    else:
        spam_pred=0
    spam_log_posterior_probabilities.append(p_spam)
    ham_log_posterior_probabilities.append(p_ham)
    print j+'\t'+str(email['spam'])+'\t'+str(spam_pred) #emit true class and class predictions for email
    if str(email['spam']) == str(spam_pred): correct_predictions+=1 #count correct predictions
    else: incorrect_predictions+=1 #count incorrect predictions

print "error rate"+'\t'+str(incorrect_predictions/(incorrect_predictions+correct_predictions))
print "spam zero probability count"+'\t'+str(spam_zero_prob_count)
print "ham zero probability count"+'\t'+str(ham_zero_prob_count)
print "spam_log_posterior_probabilities"+'\t'+str(spam_log_posterior_probabilities)
print "ham_log_posterior_probabilities"+'\t'+str(ham_log_posterior_probabilities)