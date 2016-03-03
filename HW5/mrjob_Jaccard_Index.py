#!/usr/bin/python
## top_pages_43.py
## Author: Angela Gunn 
## Description: Finds the top pages from the log


from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from sets import Set
import ast, json
import math


class Jaccard_Index(MRJob):
    
    doc_dict={} #global list
    
    def steps(self):
        return [MRStep(mapper=self.mapper_idx,
                     reducer=self.reducer_idx),
                MRStep(mapper=self.mapper_work,
                     combiner=self.reducer_work,
                     reducer=self.reducer_work),
                MRStep(mapper_init = self.mapper_calc_init,
                      mapper = self.mapper_calculator)]
    
    def mapper_idx(self, _, line):        
        tokens = line.split('\t')
        key = tokens[0].strip()
        key = key.replace('"','')
        
        
        if key[0] != "*": 
            stripe = str(tokens[1]).replace(' ','')
            #print stripe
            word_dic = ast.literal_eval(stripe)
            word_len = len(word_dic)
            words = word_dic.keys() #the words in the stripe
            i = 0
            for w1 in words:
                yield w1, (key,1)  #strword   (key, 1)
    
    def reducer_idx(self, key, value):
        words = [w for w in value]
        yield  key, words    #outputting just the key and word - no counts

    def mapper_work(self, key, doc_list):
        len_doc = len(doc_list)  #how many docs
        i = 0
        for w,c in doc_list:
            i+=1
            yield ("*",w), 1    #(*,w) is the count of that word
            for w2,c in doc_list[i:]:
                yield (w,w2), 1  #(w,w2) is the count for the co-occurance.
            
        
    def combiner_work(self, key, values):
        total = sum(values)
        yield key, total    #key and count of occurrances
        
    def reducer_work(self, key, values):
        total = sum(values)
        yield key, total    #key and count of occurrances
      
    def mapper_calc_init(self):
        self.doc_dict = {} 

    def mapper_calculator(self, key, value):
        
        x,y = key
        
        if x == '*': #|doc|
            self.doc_dict[y] = value
        else:  #at this point we have all the |doc|
            x,y = key
            words = words_sorted = [x,y]
            words_sorted.sort()
                
            if words[0] == words_sorted[0]: #words not shuffled, so process this set.
                xy = value
                calc = 1.0*xy / (self.doc_dict[x] + self.doc_dict[y] - xy)
                yield (x,y), calc
                   
if __name__ == '__main__':
    Jaccard_Index.run()