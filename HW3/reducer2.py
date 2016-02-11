#!/usr/bin/python
## reducer2.py
## Author: Jing Xu
## Description: reducer2 code for HW3.5

import sys, Queue

n_max = 50 #want top 50 largest numbers and 10 smallest numbers
q_max = Queue.Queue(n_max) #queue for largest values
total_buckets = 0 #total words used to calculate relative frequency

for line in sys.stdin:
    line = line.strip('\n') #clean line
    rec = line.split('\t', 1) #split line
    if rec[0] == '*,*': #if key is *,*, indicates it is the total buckets value
        rec[1] = rec[1].strip('\t')
        total_buckets = int(rec[1]) #assign count as total_words
    else:
        first, second = rec[0].split(',')
        rec[1] = rec[1].strip('\t')
        rec[1] = int(rec[1])
        # whatever left is the biggest
        if q_max.full():
            q_max.get()
        q_max.put([first, second, rec[1]])

top_list = []
        
for i in range(n_max):
    value = q_max.get() 
    freq = float(value[2])/float(total_buckets) #calculate relative frequency
    value.append(freq) #append relative frequency to output 
    top_list.append(value)

top_list.reverse() #reverse list so highest value is first
    
print '\nTop %d product pairs:' %n_max
for value in top_list:
    print value