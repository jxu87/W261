#!/usr/bin/python
## top_pages_43.py
## Author: Angela Gunn 
## Description: Finds the top pages from the log


from mrjob.job import MRJob
from mrjob.step import MRStep
from sets import Set
import csv
import ast
import math
import json

#then invert it: 
#   w1 {wordA:1/LA, wordB:1/LB}
#   w2 {wordA:1/LA, wordB:1/LB}
#   w3 {wordA:1/LA}
#cosine(wordA, wordB) = 1/LA*1/LB + 1/LA*1/LB = 1/LA*0


class Cosine_Inverted_Index(MRJob):
    
    global_dict={} #global list
    
    def steps(self):
        '''return [MRStep(mapper=self.mapper_idx,
                     reducer=self.reducer_idx)]'''
        return [MRStep(mapper=self.mapper_idx,
                     reducer=self.reducer_idx),
                MRStep(mapper=self.mapper_work,
                     reducer=self.reducer_work,
                      reducer_final=self.reducer_work_final)]
    
    def mapper_idx(self, _, line):   
        #coming in with 
        #   wordA {w1:#, w2:#, w3:#}
        #   wordB {w1:#, w2:#}
        #change to 
        #   wordA {w1:1, w2:1, w3:1}
        #   wordB {w1:1, w2:1}
        #get lengths
        #   LA = sqrt(1^2 + 1^2 + 1^2) = sqrt(3)
        #   LB = sqrt(1^2 + 1^2) = sqrt(2)
        #yield: 
        #   wordA {w1:1/LA}..., w2:1/LA, w3:1/LA}
        #   wordB {w1:1/LB, w2:1/LB}

        tokens = line.split('\t')
        key = tokens[0].strip()
        key = key.replace('"','') #strip quotation marks from key
        
        if key[0] != "*": 
            stripe = str(tokens[1]).replace(' ','')
            word_dic = ast.literal_eval(stripe) #convert string to word dictionary
            word_len = len(word_dic) #set length of dictionary
            words = word_dic.keys() #list of words derived from word dictionary
            length = math.sqrt(len(words)) 
            for w1 in words:
                if w1 != key:
                    value = 1.0/length
                    yield w1, {key:value}  #X {DocA,1/sqrt(len)}
    
    def reducer_idx(self, key, value):
        #coming in with:
        #    wordA [{w1:1/LA},{w2:1/LA}] 
        dic_out = {}
        for dic in value:
            for w,v in dic.iteritems():
                dic_out[w] = v
        yield key, dic_out
            
    def mapper_work(self, key, values):
        #coming in with:
        #   wordA {w1:#, w2:#, w3:#}
        #yield:
        #   w1  {wordA:#}
        #   w2  {wordA:#}
        #   w3  {wordA:#}
        for w, v in values.iteritems():
            yield w, {key:v}
       
    def reducer_work(self, key, values):
        #coming in with:
        #   w1  [{wordA:#},{wordB:#}]
        #   w2  [{wordA:#},{wordB:#}]
        #   w3  [{wordA:#}]
        #put each pair in global dic to calculate
               
        for dicA in values:
            for dicB in values:
                words = words_sorted = [dicA.keys()[0],dicB.keys()[0]]
                words_sorted.sort()
                
                if words[0] == words_sorted[0]: #words not shuffled, so process this set.
                    idx = words[0] + "," + words[1]
                    value = float(sum(dicA.values())) * float(sum(dicB.values()))
                    self.global_dict[idx] = self.global_dict.get(idx,0) + value
                            
    def reducer_work_final(self):
        #output the pairs with values.
        print len(self.global_dict)
        while len(self.global_dict)>0:
            key = max(self.global_dict, key=self.global_dict.get)
            value = self.global_dict[key]
            del self.global_dict[key]
            yield key, value
        
        
        """for idx, value in self.global_dict.iteritems():
            a,b = idx.split(',')
            yield idx, value"""
            
            
            #top_max = max(self.top5, key=self.top5.get)
  
if __name__ == '__main__':
    similarity.run()