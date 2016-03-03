#!/usr/bin/python
## mrjob_leftjoin_hw52.py
## Author: Angela Gunn & Jing Xu
## Description:Left Join
from mrjob.job import MRJob
from mrjob.step import MRStep
from mrjob.compat import get_jobconf_value

import csv

def csv_readline(line):
    """Given a sting CSV line, return a list of strings."""
    for row in csv.reader([line]):
        return row


class leftjoin(MRJob):
    def steps(self):
        return [MRStep(mapper_init = self.mapper_init,
                     mapper=self.mapper, mapper_final = self.mapper_final)]
    
    def mapper_init(self):
        #store the URLs
        self.lefttable = {}
    
        with open('url.txt') as f:
            for line in f:
                cell = csv_readline(line)
                self.lefttable[cell[1]] = (cell[4],[]) #url, list of visitors
        
    def mapper(self, _, line):
        #this is the logs
        cell = csv_readline(line) 
        key = cell[1]
        self.lefttable[key][1].append(cell[3])
        
    def mapper_final(self):
        for key, values in self.lefttable.iteritems():
            url = values[0]
            if len(values[1]) > 0:
                for u in values[1]: yield key, (url, u)
            else:
                yield key, (url, "NONE")
                
        
if __name__ == '__main__':
    leftjoin.run()