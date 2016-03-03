#!/usr/bin/python
## mrjob_frequency_53.py
## Author: Angela Gunn & Jing Xu
## Description:Find the frequency of a word in the 5-gram

from mrjob.job import MRJob
from mrjob.step import MRJobStep
from mrjob.compat import get_jobconf_value

import re

WORD_RE = re.compile(r"[A-Za-z0-9]+")

class frequency(MRJob):
    #top10={}
    
    def steps(self):
        return [
             MRJobStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer,
                   jobconf={
                            "mapred.map.tasks":16,
                            "mapred.reduce.tasks":8,
                            }),
             MRJobStep(mapper=self.mapper_frequent_unigrams,
                   reducer=self.reducer_frequent_unigrams,
                   jobconf={
                            'mapred.output.key.comparator.class': 'org.apache.hadoop.mapred.lib.KeyFieldBasedComparator',
                            'mapred.text.key.comparator.options': '-k1,1rn',
                            "mapred.map.tasks":4,
                            "mapred.reduce.tasks":1,
                            }
                   )
        ]
    
    def mapper(self, _, line):
        #get the word, and count for output
        line.strip()
        cell = re.split("\t",line)
        unigrams = cell[0].split()
        count = int(cell[1])
        for unigram in unigrams:
            yield unigram, count
            
    def combiner(self, unigram, counts):
        yield unigram, sum(counts)
        
    def reducer(self, unigram, counts):
        #combines the visits, and adds top10 dictionary if qualified
        yield unigram, sum(counts)
        
    def mapper_frequent_unigrams(self, unigram, count):
        #Just passing along with count first so that they all get shuffled by the count
        yield count, unigram
        
    def reducer_frequent_unigrams(self, count, unigrams):
        #Printing.
        for unigram in unigrams:
            yield count, unigram
        
if __name__ == '__main__':
    frequency.run()