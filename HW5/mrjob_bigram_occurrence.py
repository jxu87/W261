#!/usr/bin/python
## mrjob_bigram_occurrence.py
## Author: Jing Xu 

from mrjob.job import MRJob
from mrjob.step import MRStep
from sets import Set
import csv
import ast
import re

WORD_RE = re.compile(r"[A-Za-z0-9]+")

class bigram_occurrence(MRJob):
    
    docs = {} #initialize document dictionary
    
    def steps(self):
        return [MRStep(mapper_init = self.mapper_init,
                       mapper=self.mapper,
                     combiner=self.combiner,
                      reducer=self.reducer)]
    
    def mapper_init(self):
        self.unigrams = {} #load unigrams
        with open('1000grams.txt','r') as f:
            for line in f:
                cells = line.strip().split('\t')
                word = cells[0].replace('"','').strip()
                self.unigrams[word] = int(cells[1])
                yield "*"+word, int(cells[1]) #count of each unigram for later calculation
                                
    def mapper(self, _, line):        
        cell = line.strip().split('\t')
        words = WORD_RE.findall(cell[0])
        words = [w for w in words if w in self.unigrams.keys()] # Filter words to those in unigram list       
        for i in range(0, len(words)): 
            key = words[i] #go through each filtered word in a 5gram
            stripes = {} #initialize dictionary for all bigrams in 5gram
            for j in xrange(0, len(words)): 
                w = words[j]
                if key != w: stripes[w] = stripes.get(w,0) + 1 #if word is not itself, add to dictionary as bigram count
            if len(stripes) > 0: yield key, stripes #emit if there are bigrams
                      
    def combiner(self, key, stripes):
        dic = {}
        key = key.replace('"','')
        if key[0] == '*':
            total = sum(stripes)
            yield key, total
        else:
            for bigram in stripes:
                for word, count in bigram.iteritems():
                    word = word.replace('"','')
                    dic[word] = dic.get(word,0) + int(count) #add bigram totals together
            yield key, dic

    def reducer(self, key, stripes):
        dic = {}
        key = key.replace('"','')
        if key[0] == '*':
            total = sum(stripes)
            yield key, total
        else:
            for bigram in stripes:
                for word, count in bigram.iteritems():
                    word = word.replace('"','')
                    dic[word] = dic.get(word,0) + int(count) #add bigram totals together
            yield key, dic
                
if __name__ == '__main__':
    bigram_occurrence.run()