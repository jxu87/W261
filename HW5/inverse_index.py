#!/usr/bin/python
## inverse_index.py
## Author: Angela Gunn 
## Description: Inverses an Index.


from mrjob.job import MRJob
from mrjob.step import MRStep
import csv
from sets import Set
import ast



class inverse_index(MRJob):
    
    doc_dict={} #global list
    
    def steps(self):
        return [MRStep(mapper=self.mapper_idx,
                     combiner=self.reducer_idx)]
    
    def mapper_idx(self, _, line):        
        tokens = line.split()
        key = tokens[0].strip()
        stripe = tokens[1].strip()
        word_dic = ast.literal_eval(str(stripe))
        word_len = len(word_dic)
        words = word_dic.keys()
        i = 0
        for w1 in words:
            yield w1, (key,1)  #X, DocA
    
    def reducer_idx(self, key, value):
        dic = {}
        for w,c in value:dic[w] = c
            
        yield  key, dic


        
if __name__ == '__main__':
    inverse_index.run()