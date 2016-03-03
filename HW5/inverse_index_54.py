#!/usr/bin/python
## inverse_index_54.py
## Author: Jing Xu 
## Description: Inverses an Index.


from mrjob.job import MRJob
from mrjob.step import MRStep
from sets import Set
import csv
import ast

class inverse_index(MRJob):
    
    stripes={} #initialize dictionary of stripes
    
    def steps(self):
        return [MRStep(mapper=self.mapper,
                     combiner=self.reducer)]
    
    def mapper(self, _, line):        
        tokens = line.split()
        key = tokens[0].strip()
        stripe = tokens[1].strip()
        stripes = ast.literal_eval(str(stripe))
        word_len = len(stripes)
        words = stripes.keys()
        i = 0
        for word in words:
            yield word, (key,1)  #X, DocA
    
    def reducer(self, key, value):
        dic = {}
        for w,c in value:dic[w] = c
            
        yield  key, dic
        
if __name__ == '__main__':
    inverse_index.run()